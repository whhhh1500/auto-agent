package secretview

import "testing"

func TestPreviewBoundsRecognitionWithoutLengthDisclosure(t *testing.T) {
	if got := Preview("short"); got != fixedMask {
		t.Fatalf("short preview=%q", got)
	}
	if got := Preview("pre-middle-tail"); got != "pre"+fixedMask+"tail" {
		t.Fatalf("long preview=%q", got)
	}
}
