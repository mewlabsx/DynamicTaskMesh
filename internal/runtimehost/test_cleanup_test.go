package runtimehost

import (
	"os"
	"testing"
	"time"
)

// cleanupRuntimeHostTestDirs gives Windows a bounded release window after
// RuntimeHost/Core shutdown before removing test-owned SQLite directories.
// It is deliberately local to integration fixtures; it does not change
// production storage or RuntimeHost lifecycle behavior.
func cleanupRuntimeHostTestDirs(t *testing.T, dirs ...string) {
	t.Helper()
	for _, dir := range dirs {
		var removeErr error
		for attempt := 0; attempt < 400; attempt++ {
			removeErr = os.RemoveAll(dir)
			if removeErr == nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if removeErr != nil {
			t.Errorf("remove runtimehost test directory %s: %v", dir, removeErr)
		}
	}
}
