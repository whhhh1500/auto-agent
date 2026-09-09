package effectreceipt

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	appreceipt "github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
)

const (
	maxObjectStoreNamespaceBytes = 128
	objectStoreReceiptDomain     = "harness.object-store-receipt/v1"
	objectStoreEvidenceDomain    = "harness.object-store-evidence/v1"
)

var (
	// ErrObjectNotFound is the one portable absence signal returned by an
	// ObjectStore. Implementations may wrap it, but must not use a provider
	// response body as the error text crossing this boundary.
	ErrObjectNotFound = errors.New("effect object not found")
	ErrObjectStore    = errors.New("effect object store failure")
	ErrObjectPanic    = errors.New("effect object store panic")
	ErrObjectRequest  = errors.New("invalid effect object store request")
	ErrObjectDriver   = errors.New("invalid effect object store driver")
)

// ObjectStore is the small, provider-neutral port used by ObjectStoreDriver.
// PutIfAbsent must atomically create data at key only if key is missing and
// report whether it created the object. Get returns ErrObjectNotFound when no
// object exists. Implementations own authentication, network, and storage
// details; none are part of this adapter contract.
type ObjectStore interface {
	PutIfAbsent(context.Context, string, []byte) (created bool, err error)
	Get(context.Context, string) ([]byte, error)
}

// ObjectStoreDriver is a reference Driver whose external effect is an object
// payload at namespace/operation-key. Its confirmation is solely a later
// read-back digest comparison; a successful conditional write is accepted,
// never confirmed by itself.
type ObjectStoreDriver struct {
	ref          appreceipt.DriverRef
	namespace    string
	target       []byte
	targetDigest string
	objects      ObjectStore
}

var _ appreceipt.Driver = (*ObjectStoreDriver)(nil)

// NewObjectStoreDriver fixes an immutable namespace and caller-selected driver
// reference. This package does not provide a cloud client or a platform list:
// callers supply any ObjectStore implementation that satisfies this port.
func NewObjectStoreDriver(ref appreceipt.DriverRef, namespace string, objects ObjectStore) (*ObjectStoreDriver, error) {
	if !validDriverRef(ref) || !validNamespace(namespace) || nilInterface(objects) {
		return nil, ErrObjectDriver
	}
	target := []byte(namespace)
	return &ObjectStoreDriver{
		ref: ref, namespace: namespace, target: target, targetDigest: appreceipt.SHA256Digest(target), objects: objects,
	}, nil
}

// Ref identifies the exact read-back semantics implemented by this driver.
func (d *ObjectStoreDriver) Ref() appreceipt.DriverRef {
	if d == nil {
		return appreceipt.DriverRef{}
	}
	return d.ref
}

func (d *ObjectStoreDriver) canonicalEffectTarget() []byte {
	if d == nil {
		return nil
	}
	return append([]byte(nil), d.target...)
}

// Dispatch conditionally stores the request payload under its operation key.
// It returns only a deterministic acknowledgement digest. An existing object
// is still merely accepted: ReadBack determines whether its payload matches.
func (d *ObjectStoreDriver) Dispatch(ctx context.Context, request appreceipt.DispatchRequest) (appreceipt.Submission, error) {
	if d == nil || ctx == nil || appreceipt.ValidateDispatchRequest(request) != nil || request.Intent.Driver != d.ref ||
		request.Intent.TargetDigest != d.targetDigest || !bytes.Equal(request.Target, d.target) {
		return appreceipt.Submission{}, ErrObjectRequest
	}
	if _, err := d.putIfAbsent(ctx, d.key(request.Intent.OperationKey), request.Payload); err != nil {
		return appreceipt.Submission{}, err
	}
	return appreceipt.Submission{
		OperationKey:  request.Intent.OperationKey,
		IntentDigest:  request.Intent.IntentDigest,
		ReceiptDigest: appreceipt.SHA256Digest([]byte(objectStoreReceiptDomain + "\x00" + d.namespace + "\x00" + request.Intent.OperationKey + "\x00" + request.Intent.IntentDigest)),
	}, nil
}

// ReadBack compares a current object digest with the durable payload digest.
// Missing is pending, an exact digest is confirmed, and every different digest
// is rejected. The returned observation contains no object body or key.
func (d *ObjectStoreDriver) ReadBack(ctx context.Context, intent appreceipt.Intent) (appreceipt.Observation, error) {
	if d == nil || ctx == nil || appreceipt.ValidateIntent(intent) != nil || intent.Driver != d.ref || intent.TargetDigest != d.targetDigest {
		return appreceipt.Observation{}, ErrObjectRequest
	}
	data, err := d.get(ctx, d.key(intent.OperationKey))
	if errors.Is(err, ErrObjectNotFound) {
		return appreceipt.Observation{State: appreceipt.ObservationPending, OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest}, nil
	}
	if err != nil {
		return appreceipt.Observation{}, err
	}
	actualDigest := appreceipt.SHA256Digest(data)
	state := appreceipt.ObservationRejected
	if len(data) <= appreceipt.MaxPayloadBytes && actualDigest == intent.PayloadDigest {
		state = appreceipt.ObservationConfirmed
	}
	return appreceipt.Observation{
		State: state, OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest,
		EvidenceDigest: appreceipt.SHA256Digest([]byte(objectStoreEvidenceDomain + "\x00" + d.namespace + "\x00" + intent.OperationKey + "\x00" + intent.IntentDigest + "\x00" + actualDigest)),
	}, nil
}

func (d *ObjectStoreDriver) key(operationKey string) string { return d.namespace + "/" + operationKey }

func (d *ObjectStoreDriver) putIfAbsent(ctx context.Context, key string, payload []byte) (created bool, err error) {
	defer func() {
		if recover() != nil {
			created, err = false, ErrObjectPanic
		}
	}()
	created, err = d.objects.PutIfAbsent(ctx, key, append([]byte(nil), payload...))
	if err != nil {
		return false, ErrObjectStore
	}
	return created, nil
}

func (d *ObjectStoreDriver) get(ctx context.Context, key string) (data []byte, err error) {
	defer func() {
		if recover() != nil {
			data, err = nil, ErrObjectPanic
		}
	}()
	data, err = d.objects.Get(ctx, key)
	if errors.Is(err, ErrObjectNotFound) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, ErrObjectStore
	}
	return append([]byte(nil), data...), nil
}

func validDriverRef(ref appreceipt.DriverRef) bool {
	return validObjectText(ref.ID, 128) && validObjectText(ref.Version, 64)
}

func validNamespace(namespace string) bool {
	if !validObjectText(namespace, maxObjectStoreNamespaceBytes) || strings.HasPrefix(namespace, "/") || strings.HasSuffix(namespace, "/") || strings.Contains(namespace, "//") || strings.Contains(namespace, "..") {
		return false
	}
	for _, part := range strings.Split(namespace, "/") {
		if part == "" {
			return false
		}
	}
	return true
}

func validObjectText(value string, max int) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= max && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}
