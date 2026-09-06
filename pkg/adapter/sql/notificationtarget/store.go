// Package notificationtarget persists tenant-scoped notification target
// descriptors and opaque, encrypted provider configuration.
package notificationtarget

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	appnotification "github.com/cc-auto-agent/harness-core/pkg/app/notification"
)

const (
	maxFormatsJSONBytes   = 4096
	maxCiphertextBytes    = 256 << 10
	maxListRows           = appnotification.MaxTargets + 1
	maxRevision           = int64(^uint64(0) >> 1)
	maxEncodedConfigBytes = ((appnotification.MaxTargetConfigurationBytes + 2) / 3) * 4
)

var (
	ErrCipherRequired = errors.New("notification target cipher required")
	ErrCipherFailure  = errors.New("notification target cipher failed")
)

// Cipher seals and opens one opaque configuration value. Implementations must
// not retain plaintext input or return secrets in errors. The string-based
// seam cannot guarantee allocator-level zeroization of its temporary string;
// callers still clear all owned byte buffers at their boundaries.
type Cipher interface {
	Encrypt(string) (string, error)
	Decrypt(string) (string, error)
}

type Options struct {
	DB       *sql.DB
	Dialect  sqlkit.Dialect
	Cipher   Cipher
	Channels []appnotification.ChannelRef
}

type Store struct {
	db       *sql.DB
	dialect  sqlkit.Dialect
	cipher   Cipher
	channels map[string]appnotification.ChannelRef
}

var _ appnotification.Repository = (*Store)(nil)

func New(options Options) (*Store, error) {
	if options.DB == nil {
		return nil, errors.New("notification target store requires a database handle")
	}
	if !options.Dialect.Valid() {
		return nil, fmt.Errorf("unsupported SQL dialect %q", options.Dialect.String())
	}
	if len(options.Channels) == 0 || len(options.Channels) > appnotification.MaxChannels {
		return nil, appnotification.ErrInvalidTargetDescriptor
	}
	known := make(map[string]appnotification.ChannelRef, len(options.Channels))
	probe, err := appnotification.NewTargetRef("_")
	if err != nil {
		return nil, appnotification.ErrInvalidTargetDescriptor
	}
	for _, channel := range options.Channels {
		if _, exists := known[channelKey(channel)]; exists {
			return nil, appnotification.ErrInvalidTargetDescriptor
		}
		if err := (appnotification.TargetDescriptor{Target: probe, Channel: channel}).Validate(options.Channels); err != nil {
			return nil, appnotification.ErrInvalidTargetDescriptor
		}
		known[channelKey(channel)] = channel
	}
	return &Store{db: options.DB, dialect: options.Dialect, cipher: options.Cipher, channels: known}, nil
}

func (store *Store) List(ctx context.Context, tenantID string) ([]appnotification.TargetDescriptor, error) {
	records, err := store.ListRecords(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	result := make([]appnotification.TargetDescriptor, 0, len(records))
	for _, record := range records {
		if record.Enabled {
			result = append(result, record.Descriptor.Clone())
		}
	}
	return result, nil
}

// ListRecords reads management metadata only. config_ciphertext is
// deliberately absent from this projection, so listing never decrypts it.
func (store *Store) ListRecords(ctx context.Context, tenantID string) ([]appnotification.TargetRecord, error) {
	if err := validateContext(ctx, tenantID); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, store.bind(`SELECT target_ref, channel_id, channel_version, label, formats_json, enabled, revision
		FROM notification_targets WHERE tenant_id = ?
		ORDER BY target_ref ASC LIMIT ?`), tenantID, maxListRows)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer rows.Close()
	result := make([]appnotification.TargetRecord, 0, appnotification.MaxTargets)
	seen := make(map[string]struct{})
	for rows.Next() {
		var targetValue, channelID, channelVersion, label, formatsJSON string
		var enabled int
		var revision int64
		if err := rows.Scan(&targetValue, &channelID, &channelVersion, &label, &formatsJSON, &enabled, &revision); err != nil {
			return nil, appnotification.ErrTargetRepository
		}
		if (enabled != 0 && enabled != 1) || revision <= 0 {
			return nil, appnotification.ErrTargetRepository
		}
		target, err := appnotification.NewTargetRef(targetValue)
		if err != nil {
			return nil, appnotification.ErrTargetRepository
		}
		channel := appnotification.ChannelRef{ID: channelID, Version: channelVersion}
		descriptor := appnotification.TargetDescriptor{Target: target, Channel: channel, Label: label}
		if err := decodeFormats(formatsJSON, &descriptor.Formats); err != nil || descriptor.Validate(store.channelList()) != nil {
			return nil, appnotification.ErrTargetRepository
		}
		if _, exists := seen[targetValue]; exists {
			return nil, appnotification.ErrTargetRepository
		}
		seen[targetValue] = struct{}{}
		result = append(result, appnotification.TargetRecord{Descriptor: descriptor.Clone(), Enabled: enabled == 1, Revision: strconv.FormatInt(revision, 10)})
		if len(result) > appnotification.MaxTargets {
			return nil, appnotification.ErrTargetDirectoryCapacity
		}
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBError(err)
	}
	return result, nil
}

func (store *Store) Create(ctx context.Context, tenantID string, descriptor appnotification.TargetDescriptor, configuration appnotification.TargetConfiguration, enabled bool) (string, error) {
	if err := validateMutation(ctx, tenantID, descriptor, store.channelList(), configuration); err != nil {
		return "", err
	}
	ownedPayload := append([]byte(nil), configuration.Payload...)
	defer clearBytes(ownedPayload)
	sealed, err := store.seal(ownedPayload)
	if err != nil {
		return "", err
	}
	formats, err := encodeFormats(descriptor.Formats)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().UnixNano()
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return "", mapDBError(err)
	}
	defer tx.Rollback()
	var existing int
	err = tx.QueryRowContext(ctx, store.bind(`SELECT 1 FROM notification_targets WHERE tenant_id = ? AND target_ref = ?`), tenantID, descriptor.Target.String()).Scan(&existing)
	switch {
	case err == nil:
		return "", appnotification.ErrTargetRevisionConflict
	case !errors.Is(err, sql.ErrNoRows):
		return "", mapDBError(err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, store.bind(`SELECT COUNT(*) FROM notification_targets WHERE tenant_id = ?`), tenantID).Scan(&count); err != nil {
		return "", mapDBError(err)
	}
	if count >= appnotification.MaxTargets {
		return "", appnotification.ErrTargetDirectoryCapacity
	}
	result, err := tx.ExecContext(ctx, store.bind(`INSERT INTO notification_targets
		(tenant_id, target_ref, channel_id, channel_version, label, formats_json, config_ciphertext, enabled, revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
		ON CONFLICT (tenant_id, target_ref) DO NOTHING`), tenantID, descriptor.Target.String(), descriptor.Channel.ID,
		descriptor.Channel.Version, descriptor.Label, formats, sealed, boolInt(enabled), now, now)
	if err != nil {
		return "", mapDBError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", appnotification.ErrTargetRepository
	}
	if affected != 1 {
		return "", appnotification.ErrTargetRevisionConflict
	}
	if err := tx.Commit(); err != nil {
		return "", mapDBError(err)
	}
	return "1", nil
}

func (store *Store) Update(ctx context.Context, tenantID string, descriptor appnotification.TargetDescriptor, configuration appnotification.TargetConfiguration, enabled bool, expectedRevision string) (string, error) {
	if err := validateMutation(ctx, tenantID, descriptor, store.channelList(), configuration); err != nil {
		return "", err
	}
	expected, err := parseRevision(expectedRevision)
	if err != nil {
		return "", err
	}
	formats, err := encodeFormats(descriptor.Formats)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().UnixNano()
	var result sql.Result
	if configuration.Payload == nil {
		result, err = store.db.ExecContext(ctx, store.bind(`UPDATE notification_targets SET
			channel_id = ?, channel_version = ?, label = ?, formats_json = ?,
			enabled = ?, revision = revision + 1, updated_at = ?
			WHERE tenant_id = ? AND target_ref = ? AND revision = ?
			AND channel_id = ? AND channel_version = ?`), descriptor.Channel.ID, descriptor.Channel.Version,
			descriptor.Label, formats, boolInt(enabled), now, tenantID, descriptor.Target.String(), expected,
			descriptor.Channel.ID, descriptor.Channel.Version)
	} else {
		ownedPayload := append([]byte(nil), configuration.Payload...)
		defer clearBytes(ownedPayload)
		sealed, sealErr := store.seal(ownedPayload)
		if sealErr != nil {
			return "", sealErr
		}
		result, err = store.db.ExecContext(ctx, store.bind(`UPDATE notification_targets SET
			channel_id = ?, channel_version = ?, label = ?, formats_json = ?, config_ciphertext = ?,
			enabled = ?, revision = revision + 1, updated_at = ?
			WHERE tenant_id = ? AND target_ref = ? AND revision = ?`), descriptor.Channel.ID, descriptor.Channel.Version,
			descriptor.Label, formats, sealed, boolInt(enabled), now, tenantID, descriptor.Target.String(), expected)
	}
	if err != nil {
		return "", mapDBError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", appnotification.ErrTargetRepository
	}
	if affected == 1 {
		return strconv.FormatInt(expected+1, 10), nil
	}
	if configuration.Payload == nil {
		return "", store.classifyPreservedUpdateFailure(ctx, tenantID, descriptor, expected)
	}
	return "", store.classifyMissingOrConflict(ctx, tenantID, descriptor.Target)
}

func (store *Store) Delete(ctx context.Context, tenantID string, target appnotification.TargetRef, expectedRevision string) error {
	if err := validateContext(ctx, tenantID); err != nil {
		return err
	}
	if _, err := appnotification.NewTargetRef(target.String()); err != nil {
		return appnotification.ErrInvalidTargetDescriptor
	}
	expected, err := parseRevision(expectedRevision)
	if err != nil {
		return err
	}
	result, err := store.db.ExecContext(ctx, store.bind(`DELETE FROM notification_targets
		WHERE tenant_id = ? AND target_ref = ? AND revision = ?`), tenantID, target.String(), expected)
	if err != nil {
		return mapDBError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return appnotification.ErrTargetRepository
	}
	if affected == 1 {
		return nil
	}
	return store.classifyMissingOrConflict(ctx, tenantID, target)
}

// ResolveConfig is deliberately absent from List and returns only an owned
// opaque payload after exact tenant/target/channel and enabled checks.
func (store *Store) ResolveConfig(ctx context.Context, tenantID string, target appnotification.TargetRef, channel appnotification.ChannelRef) (appnotification.TargetConfiguration, error) {
	if err := validateContext(ctx, tenantID); err != nil {
		return appnotification.TargetConfiguration{}, err
	}
	if _, err := appnotification.NewTargetRef(target.String()); err != nil {
		return appnotification.TargetConfiguration{}, appnotification.ErrInvalidTargetDescriptor
	}
	if _, known := store.channels[channelKey(channel)]; !known {
		return appnotification.TargetConfiguration{}, appnotification.ErrUnknownTargetChannel
	}
	var storedID, storedVersion, ciphertext string
	var enabled int
	err := store.db.QueryRowContext(ctx, store.bind(`SELECT channel_id, channel_version, config_ciphertext, enabled
		FROM notification_targets WHERE tenant_id = ? AND target_ref = ?`), tenantID, target.String()).Scan(&storedID, &storedVersion, &ciphertext, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return appnotification.TargetConfiguration{}, appnotification.ErrTargetNotFound
	}
	if err != nil {
		return appnotification.TargetConfiguration{}, mapDBError(err)
	}
	if storedID != channel.ID || storedVersion != channel.Version {
		return appnotification.TargetConfiguration{}, appnotification.ErrUnknownTargetChannel
	}
	if enabled == 0 {
		return appnotification.TargetConfiguration{}, appnotification.ErrTargetDisabled
	}
	if ciphertext == "" {
		return appnotification.TargetConfiguration{}, nil
	}
	if store.cipher == nil {
		return appnotification.TargetConfiguration{}, ErrCipherRequired
	}
	opened, err := safeDecrypt(store.cipher, ciphertext)
	if err != nil {
		return appnotification.TargetConfiguration{}, ErrCipherFailure
	}
	openedBytes := []byte(opened)
	defer clearBytes(openedBytes)
	if len(openedBytes) > maxEncodedConfigBytes || !utf8.Valid(openedBytes) {
		return appnotification.TargetConfiguration{}, ErrCipherFailure
	}
	decoded, err := base64.StdEncoding.DecodeString(opened)
	if err != nil || len(decoded) > appnotification.MaxTargetConfigurationBytes {
		clearBytes(decoded)
		return appnotification.TargetConfiguration{}, ErrCipherFailure
	}
	return appnotification.TargetConfiguration{Payload: decoded}, nil
}

func (store *Store) seal(payload []byte) (string, error) {
	if len(payload) == 0 {
		return "", nil
	}
	if store.cipher == nil {
		return "", ErrCipherRequired
	}
	encodedBytes := make([]byte, base64.StdEncoding.EncodedLen(len(payload)))
	base64.StdEncoding.Encode(encodedBytes, payload)
	defer clearBytes(encodedBytes)
	sealed, err := safeEncrypt(store.cipher, string(encodedBytes))
	if err != nil || sealed == "" || len(sealed) > maxCiphertextBytes || !utf8.ValidString(sealed) {
		return "", ErrCipherFailure
	}
	return sealed, nil
}

func (store *Store) classifyMissingOrConflict(ctx context.Context, tenantID string, target appnotification.TargetRef) error {
	var revision int64
	err := store.db.QueryRowContext(ctx, store.bind(`SELECT revision FROM notification_targets WHERE tenant_id = ? AND target_ref = ?`), tenantID, target.String()).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return appnotification.ErrTargetNotFound
	}
	if err != nil {
		return mapDBError(err)
	}
	return appnotification.ErrTargetRevisionConflict
}

func (store *Store) classifyPreservedUpdateFailure(ctx context.Context, tenantID string, descriptor appnotification.TargetDescriptor, expected int64) error {
	var revision int64
	var channelID, channelVersion string
	err := store.db.QueryRowContext(ctx, store.bind(`SELECT channel_id, channel_version, revision
		FROM notification_targets WHERE tenant_id = ? AND target_ref = ?`), tenantID, descriptor.Target.String()).Scan(&channelID, &channelVersion, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return appnotification.ErrTargetNotFound
	}
	if err != nil {
		return mapDBError(err)
	}
	if revision == expected && (channelID != descriptor.Channel.ID || channelVersion != descriptor.Channel.Version) {
		return appnotification.ErrTargetChannelChangeRequiresConfiguration
	}
	return appnotification.ErrTargetRevisionConflict
}

func (store *Store) channelList() []appnotification.ChannelRef {
	result := make([]appnotification.ChannelRef, 0, len(store.channels))
	for _, channel := range store.channels {
		result = append(result, channel)
	}
	return result
}

func (store *Store) bind(query string) string {
	bound, err := sqlkit.Bind(query, store.dialect)
	if err != nil {
		panic("notification target SQL binding invariant violated: " + err.Error())
	}
	return bound
}

func validateMutation(ctx context.Context, tenantID string, descriptor appnotification.TargetDescriptor, channels []appnotification.ChannelRef, configuration appnotification.TargetConfiguration) error {
	if err := validateContext(ctx, tenantID); err != nil {
		return err
	}
	if err := descriptor.Validate(channels); err != nil {
		return err
	}
	if len(configuration.Payload) > appnotification.MaxTargetConfigurationBytes {
		return appnotification.ErrInvalidTargetConfiguration
	}
	return nil
}

func validateContext(ctx context.Context, tenantID string) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return appnotification.ValidateTenantID(tenantID)
}

func encodeFormats(formats []string) (string, error) {
	// Persist an omitted optional list as a JSON array, not null. The read
	// boundary rejects null because it is not a valid descriptor collection.
	copyFormats := append([]string{}, formats...)
	encoded, err := json.Marshal(copyFormats)
	if err != nil || len(encoded) > maxFormatsJSONBytes {
		return "", appnotification.ErrInvalidTargetDescriptor
	}
	return string(encoded), nil
}

func decodeFormats(raw string, formats *[]string) error {
	if raw == "" || len(raw) > maxFormatsJSONBytes {
		return appnotification.ErrInvalidTargetDescriptor
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "null" || trimmed[0] != '[' {
		return appnotification.ErrInvalidTargetDescriptor
	}
	var decoded []string
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil || decoded == nil {
		return appnotification.ErrInvalidTargetDescriptor
	}
	*formats = decoded
	return nil
}

func parseRevision(value string) (int64, error) {
	if value == "" {
		return 0, appnotification.ErrInvalidTargetRevision
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 || parsed >= maxRevision {
		return 0, appnotification.ErrInvalidTargetRevision
	}
	return parsed, nil
}

func channelKey(channel appnotification.ChannelRef) string {
	return channel.ID + "\x00" + channel.Version
}
func boolInt(enabled bool) int {
	if enabled {
		return 1
	}
	return 0
}

func mapDBError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return appnotification.ErrTargetRepository
}

func safeEncrypt(cipher Cipher, value string) (result string, err error) {
	defer func() {
		if recover() != nil {
			result, err = "", ErrCipherFailure
		}
	}()
	result, err = cipher.Encrypt(value)
	if err != nil {
		return "", ErrCipherFailure
	}
	return result, nil
}

func safeDecrypt(cipher Cipher, value string) (result string, err error) {
	defer func() {
		if recover() != nil {
			result, err = "", ErrCipherFailure
		}
	}()
	result, err = cipher.Decrypt(value)
	if err != nil {
		return "", ErrCipherFailure
	}
	return result, nil
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
