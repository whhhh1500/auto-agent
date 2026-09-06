package buildinfo

import "testing"

func TestBuildInfoDefaultsAreUseful(t *testing.T) {
	if Version == "" || Commit == "" || BuildDate == "" {
		t.Fatalf("build info defaults must be non-empty: version=%q commit=%q date=%q", Version, Commit, BuildDate)
	}
	if String() == "" {
		t.Fatal("build info string is empty")
	}
}
