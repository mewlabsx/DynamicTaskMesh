//go:build !dtm_test_submission_barrier

package main

import "dtm/internal/application"

func processSubmissionBarrierOptions() []application.TaskServiceOption { return nil }
