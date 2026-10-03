//go:build dtm_test_submission_barrier

package main

import (
	"os"
	"strings"
	"time"

	"dtm/internal/application"
)

const (
	processSubmissionAcceptedFile = "DTM_TEST_SUBMISSION_ACCEPTED_FILE"
	processSubmissionReleaseFile  = "DTM_TEST_SUBMISSION_RELEASE_FILE"
)

func processSubmissionBarrierOptions() []application.TaskServiceOption {
	acceptedPath := strings.TrimSpace(os.Getenv(processSubmissionAcceptedFile))
	releasePath := strings.TrimSpace(os.Getenv(processSubmissionReleaseFile))
	if acceptedPath == "" || releasePath == "" {
		return nil
	}
	return []application.TaskServiceOption{application.WithAfterTaskAcceptedHook(func(application.TaskRecord) {
		_ = os.WriteFile(acceptedPath, []byte("accepted\n"), 0o600)
		for {
			if _, err := os.Stat(releasePath); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})}
}
