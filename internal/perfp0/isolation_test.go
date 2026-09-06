package perfp0

import (
	"strings"
	"testing"
)

func TestDecodeChildResponseRejectsLogPollution(t *testing.T) {
	_, err := decodeChildResponse([]byte("debug: child started\n{}"))
	if err == nil || err.Error() != "isolated case protocol failed" {
		t.Fatalf("expected fixed protocol error, got %v", err)
	}
}

func TestDecodeChildResponseBoundsOutputAndRedactsChildError(t *testing.T) {
	_, err := decodeChildResponse([]byte(strings.Repeat("x", maxChildJSONBytes+1)))
	if err == nil || err.Error() != "isolated case protocol exceeded limit" {
		t.Fatalf("expected size error, got %v", err)
	}
	_, err = decodeChildResponse([]byte(`{"error":"provider secret=do-not-leak"}`))
	if err == nil || err.Error() != "isolated case child failed" || strings.Contains(err.Error(), "secret") {
		t.Fatalf("child error leaked or was not classified: %v", err)
	}
}

func TestDecodeChildResponseRequiresResult(t *testing.T) {
	_, err := decodeChildResponse([]byte(`{}`))
	if err == nil || err.Error() != "isolated case returned no result" {
		t.Fatalf("expected missing-result error, got %v", err)
	}
}
