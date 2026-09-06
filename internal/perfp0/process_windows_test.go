//go:build windows

package perfp0

import "testing"

func TestReadProcessSampleWindows(t *testing.T) {
	sample, err := readProcessSample()
	if err != nil {
		t.Fatal(err)
	}
	if sample.RSSBytes == 0 {
		t.Fatal("working set was not reported")
	}
}
