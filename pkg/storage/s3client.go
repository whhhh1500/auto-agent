package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// S3Config configures one S3-compatible bucket endpoint. It works against
// AWS S3, MinIO, Cloudflare R2, and S3-compatible OSS/COS gateways.
type S3Config struct {
	Endpoint  string // e.g. https://s3.us-east-1.amazonaws.com or http://minio:9000
	Region    string // e.g. us-east-1; MinIO commonly accepts any non-empty value
	Bucket    string
	AccessKey string
	SecretKey string
	// PathStyle uses endpoint/bucket/key addressing. Required by MinIO and
	// most self-hosted gateways; virtual-host style is the AWS default.
	PathStyle bool
	// HTTPClient is optional; a 30s client is used when nil.
	HTTPClient *http.Client
}

// S3ObjectStore speaks the minimal S3 REST surface (SigV4) that the session
// store needs, including conditional PUTs via x-amz-if-match and
// x-amz-if-none-match. It intentionally has no SDK dependency: the session
// store only uses get/put/delete/list, and a 300-line client keeps the open
// source runtime free of a heavy dependency tree.
type S3ObjectStore struct {
	cfg  S3Config
	http *http.Client
}

func NewS3ObjectStore(cfg S3Config) (*S3ObjectStore, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("s3 endpoint, bucket and credentials are required")
	}
	if !strings.HasPrefix(cfg.Endpoint, "http://") && !strings.HasPrefix(cfg.Endpoint, "https://") {
		return nil, fmt.Errorf("s3 endpoint must include a scheme")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &S3ObjectStore{cfg: cfg, http: httpClient}, nil
}

// S3Error carries the HTTP status so callers can map failure classes.
type S3Error struct {
	Status int
	Code   string
	Body   string
}

func (e *S3Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("s3: status %d code %s: %s", e.Status, e.Code, e.Body)
	}
	return fmt.Sprintf("s3: status %d: %s", e.Status, e.Body)
}

var (
	// ErrS3ResponseTooLarge indicates that an S3 control response exceeded the
	// bounded response size for its operation. Object bodies use Open/Get and
	// are governed by their separate streaming/byte-slice limits.
	ErrS3ResponseTooLarge = errors.New("s3 response exceeds maximum size")
	errS3NotFound         = fmt.Errorf("%w: s3 object", ErrObjectNotFound)
	errS3Precondition     = fmt.Errorf("%w: s3 conditional write", ErrPreconditionFailed)
)

const (
	// ListBucket responses contain up to 1000 entries and may include keys,
	// ETags, sizes, and timestamps. Keep this generous enough for a normal
	// page while preventing a malformed gateway from forcing an unbounded
	// allocation.
	maxS3ListResponseBytes int64 = 8 << 20
	// PUT, DELETE, bucket creation, and existence probes have no meaningful
	// large success payload. Some compatible gateways return a small XML body,
	// so retain a modest compatibility allowance.
	maxS3SuccessResponseBytes int64 = 64 << 10
)

func s3SuccessResponseLimit(method, query string) int64 {
	if method == http.MethodGet && strings.Contains(query, "list-type=2") {
		return maxS3ListResponseBytes
	}
	return maxS3SuccessResponseBytes
}

func s3ResponseTooLarge(method string, limit int64) error {
	return fmt.Errorf("%w: %s response exceeds %d bytes", ErrS3ResponseTooLarge, method, limit)
}

func objectError(resp *http.Response, body []byte) error {
	var doc struct {
		Code string `xml:"Code"`
	}
	_ = xml.Unmarshal(body, &doc)
	err := &S3Error{Status: resp.StatusCode, Code: doc.Code, Body: truncate(string(body), 300)}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%w: %s", errS3NotFound, err)
	case resp.StatusCode == http.StatusPreconditionFailed:
		return fmt.Errorf("%w: %s", errS3Precondition, err)
	case resp.StatusCode == http.StatusNotImplemented:
		return fmt.Errorf("%w: %s", ErrConditionUnsupported, err)
	default:
		return err
	}
}

func (s *S3ObjectStore) url(key, query string) string {
	base := strings.TrimRight(s.cfg.Endpoint, "/")
	if s.cfg.PathStyle {
		u := base + "/" + s.cfg.Bucket
		if key != "" {
			u += "/" + s3EscapePath(key)
		}
		if query != "" {
			u += "?" + query
		}
		return u
	}
	host := strings.TrimPrefix(strings.TrimPrefix(base, "https://"), "http://")
	scheme := "https"
	if strings.HasPrefix(base, "http://") {
		scheme = "http"
	}
	u := scheme + "://" + s.cfg.Bucket + "." + host
	if key != "" {
		u += "/" + s3EscapePath(key)
	}
	if query != "" {
		u += "?" + query
	}
	return u
}

func (s *S3ObjectStore) hostHeader() string {
	host := strings.TrimPrefix(strings.TrimPrefix(s.cfg.Endpoint, "https://"), "http://")
	if !s.cfg.PathStyle {
		host = s.cfg.Bucket + "." + host
	}
	return host
}

func (s *S3ObjectStore) newRequest(ctx context.Context, method, key, query string, body []byte, extraHeaders map[string]string) (*http.Request, error) {
	return s.newRequestBody(ctx, method, key, query, bytes.NewReader(body), sha256Hex(body), extraHeaders)
}

func (s *S3ObjectStore) newRequestBody(ctx context.Context, method, key, query string, body io.Reader, payloadHash string, extraHeaders map[string]string) (*http.Request, error) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	fullPath := "/" + s.cfg.Bucket
	if key != "" {
		fullPath += "/" + s3EscapePath(key)
	}
	// The canonical URI includes the bucket for path-style addressing and is
	// just the key path for virtual-host addressing.
	canonicalURI := "/" + s3EscapePath(key)
	if s.cfg.PathStyle {
		canonicalURI = fullPath
	}

	signed := map[string]string{
		"host":                 s.hostHeader(),
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	for name, value := range extraHeaders {
		signed[strings.ToLower(name)] = value
	}
	names := make([]string, 0, len(signed))
	for name := range signed {
		names = append(names, name)
	}
	sort.Strings(names)
	signedHeaders := strings.Join(names, ";")

	canonicalHeaders := ""
	for _, name := range names {
		canonicalHeaders += name + ":" + strings.TrimSpace(signed[name]) + "\n"
	}
	canonicalRequest := strings.Join([]string{
		method, canonicalURI, canonicalQueryString(query), canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")
	scope := strings.Join([]string{dateStamp, s.cfg.Region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signingKey := hmacSHA256(hmacSHA256(hmacSHA256(hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), dateStamp), s.cfg.Region), "s3"), "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	request, err := http.NewRequestWithContext(ctx, method, s.url(key, query), body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.cfg.AccessKey, scope, signedHeaders, signature,
	))
	for name, value := range signed {
		if name != "host" {
			request.Header.Set(name, value)
		}
	}
	return request, nil
}

func (s *S3ObjectStore) do(ctx context.Context, method, key, query string, body []byte, extraHeaders map[string]string) (*http.Response, []byte, error) {
	request, err := s.newRequest(ctx, method, key, query, body, extraHeaders)
	if err != nil {
		return nil, nil, err
	}

	resp, err := s.http.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		// Preserve the existing bounded diagnostic behavior for errors. Do not
		// apply the success-operation limit here: callers rely on S3 error
		// mapping and the diagnostic body is intentionally capped at 64 KiB.
		respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, objectError(resp, respBody)
	}
	limit := s3SuccessResponseLimit(method, query)
	if resp.ContentLength > limit {
		return nil, nil, s3ResponseTooLarge(method, limit)
	}
	// Read one byte beyond the accepted limit so unknown-length responses are
	// rejected deterministically without retaining their full body.
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, nil, err
	}
	if int64(len(respBody)) > limit {
		return nil, nil, s3ResponseTooLarge(method, limit)
	}
	return resp, respBody, nil
}

// Open streams an S3 object while retaining SigV4's signed empty GET payload.
// Error bodies are bounded by a 64 KiB diagnostic limit. A cold
// session-event sibling uses the legacy decompression fallback, while normal
// resources never materialize the response.
func (s *S3ObjectStore) Open(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	if err := ValidateObjectKey(key); err != nil {
		return nil, "", 0, err
	}
	request, err := s.newRequest(ctx, http.MethodGet, key, "", nil, nil)
	if err != nil {
		return nil, "", 0, err
	}
	resp, err := s.http.Do(request)
	if err != nil {
		return nil, "", 0, err
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return nil, "", 0, readErr
		}
		if _, _, cold := sessionEventObjectKey(key); cold && resp.StatusCode == http.StatusNotFound {
			zstBody, zstErr := s.fetchDecompressed(ctx, key+".zst")
			if zstErr == nil {
				return io.NopCloser(bytes.NewReader(zstBody)), contentETag(zstBody), int64(len(zstBody)), nil
			}
		}
		return nil, "", 0, objectError(resp, body)
	}
	return resp.Body, resp.Header.Get("ETag"), resp.ContentLength, nil
}

func (s *S3ObjectStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return nil, "", err
	}
	body, etag, size, err := s.Open(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer body.Close()
	if size > MaxObjectBytes {
		return nil, "", objectTooLarge(MaxObjectBytes)
	}
	data, readErr := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: body}, MaxObjectBytes+1))
	if readErr != nil {
		return nil, "", readErr
	}
	if len(data) > MaxObjectBytes {
		return nil, "", objectTooLarge(MaxObjectBytes)
	}
	return data, etag, nil
}

// fetchDecompressed gets a zstd-compressed object and expands it.
func (s *S3ObjectStore) fetchDecompressed(ctx context.Context, key string) ([]byte, error) {
	if err := ValidateObjectKey(key); err != nil {
		return nil, err
	}
	body, _, _, err := s.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return decompressZstdReaderBounded(contextReader{ctx: ctx, reader: body}, MaxObjectBytes)
}

func (s *S3ObjectStore) Put(ctx context.Context, key string, data []byte, opts PutOptions) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	if opts.IfMatch != "" && opts.IfNoneMatchStar {
		return "", errors.New("s3 put: IfMatch and IfNoneMatchStar are mutually exclusive")
	}
	if len(data) > MaxObjectBytes {
		return "", objectTooLarge(MaxObjectBytes)
	}
	if opts.IfNoneMatchStar && isSessionEventObjectKey(key) {
		// S3 can assert non-existence for only one physical key. A cold event
		// may exist solely as key+".zst", so inspect that alternate immutable
		// representation before issuing If-None-Match against the plain key.
		if _, _, err := s.do(ctx, http.MethodGet, key+".zst", "", nil, nil); err == nil {
			return "", fmt.Errorf("%w: immutable session event %s is cold", ErrPreconditionFailed, key)
		} else if !errors.Is(err, errS3NotFound) {
			return "", err
		}
	}
	headers := map[string]string{}
	if opts.IfMatch != "" {
		headers["x-amz-if-match"] = opts.IfMatch
	}
	if opts.IfNoneMatchStar {
		headers["x-amz-if-none-match"] = "*"
	}
	resp, _, err := s.do(ctx, http.MethodPut, key, "", data, headers)
	if err != nil {
		return "", err
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		etag = contentETag(data)
	}
	// The primary PUT has succeeded. Best-effort cleanup keeps stale generic
	// cold siblings from accumulating without changing successful PUT/CAS
	// semantics when that cleanup is unavailable.
	if sibling, ok := zstdSibling(key); ok {
		_, _, _ = s.do(ctx, http.MethodDelete, sibling, "", nil, nil)
	}
	return etag, nil
}

// PutStream spools to a private temporary file so SigV4 can retain an exact
// payload hash without retaining the object in memory. The temp path is
// absolute, mode-restricted, and removed on every success, failure, or
// cancellation path.
func (s *S3ObjectStore) PutStream(ctx context.Context, key string, body io.Reader, maxBytes int64, opts PutOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	if opts.IfMatch != "" && opts.IfNoneMatchStar {
		return "", errors.New("s3 put: IfMatch and IfNoneMatchStar are mutually exclusive")
	}
	if opts.IfNoneMatchStar && isSessionEventObjectKey(key) {
		if _, _, err := s.do(ctx, http.MethodGet, key+".zst", "", nil, nil); err == nil {
			return "", fmt.Errorf("%w: immutable session event %s is cold", ErrPreconditionFailed, key)
		} else if !errors.Is(err, errS3NotFound) {
			return "", err
		}
	}
	limit := streamingObjectLimit(maxBytes)
	tempRoot, err := filepath.Abs(os.TempDir())
	if err != nil || !filepath.IsAbs(tempRoot) {
		return "", fmt.Errorf("resolve absolute S3 stream temp directory: %w", err)
	}
	tmp, err := os.CreateTemp(tempRoot, ".harness-s3-stream-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err := privateChmod(tmpName, 0o600); err != nil {
		_ = tmp.Close()
		return "", err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(contextReader{ctx: ctx, reader: body}, limit+1))
	if copyErr != nil {
		_ = tmp.Close()
		return "", copyErr
	}
	if err := ctx.Err(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if written > limit {
		_ = tmp.Close()
		return "", objectTooLarge(limit)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return "", err
	}
	payloadHash := hex.EncodeToString(hash.Sum(nil))
	// Hide the file's Close method from net/http; PutStream owns cleanup even
	// after the transport has finished consuming the request body.
	request, err := s.newRequestBody(ctx, http.MethodPut, key, "", readerOnly{Reader: tmp}, payloadHash, s3PutHeaders(opts))
	if err != nil {
		_ = tmp.Close()
		return "", err
	}
	request.ContentLength = written
	resp, err := s.http.Do(request)
	if err != nil {
		_ = tmp.Close()
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		errorBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = tmp.Close()
		if readErr != nil {
			return "", readErr
		}
		return "", objectError(resp, errorBody)
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		etag = `"` + payloadHash + `"`
	}
	if sibling, ok := zstdSibling(key); ok {
		_, _, _ = s.do(ctx, http.MethodDelete, sibling, "", nil, nil)
	}
	return etag, nil
}

type readerOnly struct {
	io.Reader
}

func s3PutHeaders(opts PutOptions) map[string]string {
	headers := map[string]string{}
	if opts.IfMatch != "" {
		headers["x-amz-if-match"] = opts.IfMatch
	}
	if opts.IfNoneMatchStar {
		headers["x-amz-if-none-match"] = "*"
	}
	return headers
}

// EnsureBucket creates the configured bucket if it does not already exist.
// Idempotent: an existing bucket (409 Conflict) is treated as success.
func (s *S3ObjectStore) EnsureBucket(ctx context.Context) error {
	_, _, err := s.do(ctx, http.MethodPut, "", "", nil, nil)
	if err == nil {
		return nil
	}
	var s3Err *S3Error
	if errors.As(err, &s3Err) && (s3Err.Status == http.StatusConflict || s3Err.Status == http.StatusOK) {
		return nil // BucketAlreadyOwnedByYou / already exists
	}
	return err
}

func (s *S3ObjectStore) Delete(ctx context.Context, key string) error {
	if err := ValidateObjectKey(key); err != nil {
		return err
	}
	if sibling, ok := zstdSibling(key); ok {
		if _, _, err := s.do(ctx, http.MethodDelete, sibling, "", nil, nil); err != nil {
			return err
		}
	}
	_, _, err := s.do(ctx, http.MethodDelete, key, "", nil, nil)
	return err
}

func (s *S3ObjectStore) deleteColdPlain(ctx context.Context, key string) error {
	if err := ValidateObjectKey(key); err != nil {
		return err
	}
	_, _, err := s.do(ctx, http.MethodDelete, key, "", nil, nil)
	return err
}

type s3ListBucketResult struct {
	IsTruncated    bool   `xml:"IsTruncated"`
	NextStartAfter string `xml:"NextStartAfter"`
	Contents       []struct {
		Key          string    `xml:"Key"`
		ETag         string    `xml:"ETag"`
		Size         int64     `xml:"Size"`
		LastModified time.Time `xml:"LastModified"`
	} `xml:"Contents"`
}

func (s *S3ObjectStore) List(ctx context.Context, prefix, startAfter string, limit int) ([]ObjectItem, error) {
	if prefix != "" {
		if err := ValidateObjectKey(strings.TrimSuffix(prefix, "/")); err != nil {
			return nil, err
		}
	}
	items := []ObjectItem{}
	cursor := startAfter
	for {
		if limit > 0 && len(items) >= limit {
			break
		}
		maxKeys := 1000
		if limit > 0 {
			maxKeys = limit - len(items)
		}
		query := "list-type=2&prefix=" + queryEscape(prefix)
		if cursor != "" {
			query += "&start-after=" + queryEscape(cursor)
		}
		query += "&max-keys=" + strconv.Itoa(maxKeys)
		_, body, err := s.do(ctx, http.MethodGet, "", query, nil, nil)
		if err != nil {
			return nil, err
		}
		var result s3ListBucketResult
		if err := xml.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("s3 list: decode response: %w", err)
		}
		for _, entry := range result.Contents {
			items = append(items, ObjectItem{Key: entry.Key, ETag: entry.ETag, Size: entry.Size, ModTime: entry.LastModified})
			cursor = entry.Key
		}
		if !result.IsTruncated || len(result.Contents) == 0 {
			break
		}
	}
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

// ----- SigV4 helpers -----

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// s3EscapePath escapes each path segment while preserving "/" separators.
func s3EscapePath(key string) string {
	segments := strings.Split(key, "/")
	for i, segment := range segments {
		segments[i] = queryEscape(segment)
	}
	return strings.Join(segments, "/")
}

// canonicalQueryString sorts pre-encoded "name=value" pairs. Values must
// already be query-escaped by the caller (List builds them that way); SigV4
// requires the canonical form to match the actual request query byte-for-byte.
func canonicalQueryString(query string) string {
	if query == "" {
		return ""
	}
	pairs := strings.Split(query, "&")
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func queryEscape(value string) string {
	const hexDigits = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'),
			c == '-', c == '_', c == '.', c == '~':
			out.WriteByte(c)
		default:
			out.WriteByte('%')
			out.WriteByte(hexDigits[c>>4])
			out.WriteByte(hexDigits[c&0xf])
		}
	}
	return out.String()
}

func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit]
}
