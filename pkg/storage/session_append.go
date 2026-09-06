package storage

import (
	"fmt"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func validateAppendEvents(expectedVersion int64, events []core.SessionEvent) error {
	for index, event := range events {
		want := expectedVersion + int64(index)
		if event.Seq != want {
			return fmt.Errorf("event append is discontinuous: expected seq %d, found %d", want, event.Seq)
		}
	}
	return nil
}
