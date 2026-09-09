package toolcapability

import (
	"errors"
	"testing"
)

func TestToolDataPreservesTextWithoutInterpretingJSONPrefixes(t *testing.T) {
	for _, content := range []string{"notification delivered", "42 results", "", `{"ok":true} trailing text`} {
		data, err := toolData(content)
		if err != nil || data != nil {
			t.Fatalf("plain text must remain content-only: data=%v err=%v", data, err)
		}
	}
	if _, err := toolData(`{"allowed":false,"allowed":true}`); !errors.Is(err, errToolData) {
		t.Fatalf("ambiguous JSON must not become program data: %v", err)
	}
}
