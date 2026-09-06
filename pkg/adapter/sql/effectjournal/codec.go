package effectjournal

import (
	"bytes"
	"encoding/base64"

	"github.com/cc-auto-agent/harness-core/pkg/runtime"
)

func sameDescriptor(left, right runtime.EffectDescriptor) bool {
	return left.ID == right.ID && left.ModuleID == right.ModuleID && left.ModuleRevision == right.ModuleRevision &&
		left.CompositionRevision == right.CompositionRevision && left.Phase == right.Phase &&
		left.Forward.Kind == right.Forward.Kind && left.Forward.Target == right.Forward.Target &&
		bytes.Equal(left.Forward.Payload, right.Forward.Payload) && left.Inverse.Kind == right.Inverse.Kind &&
		left.Inverse.Target == right.Inverse.Target && bytes.Equal(left.Inverse.Payload, right.Inverse.Payload)
}

func cloneDescriptor(descriptor runtime.EffectDescriptor) runtime.EffectDescriptor {
	descriptor.Forward.Payload = append([]byte(nil), descriptor.Forward.Payload...)
	descriptor.Inverse.Payload = append([]byte(nil), descriptor.Inverse.Payload...)
	return descriptor
}

func cloneRecordedEffect(record runtime.RecordedEffect) runtime.RecordedEffect {
	record.Descriptor = cloneDescriptor(record.Descriptor)
	return record
}

func encodePayload(payload []byte) string { return base64.StdEncoding.EncodeToString(payload) }

func decodePayload(payload string) ([]byte, error) { return base64.StdEncoding.DecodeString(payload) }
