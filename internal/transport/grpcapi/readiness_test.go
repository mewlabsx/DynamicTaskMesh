package grpcapi

import (
	"context"
	"testing"

	dtmv1 "dtm/api/proto/dtm/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeCoreReadinessGateBlocksTaskAndQueryIngress(t *testing.T) {
	ready := false
	server, err := NewCoreServer(
		&recordingTaskSubmitter{},
		WithTaskQueryService(&recordingTaskQueries{}),
		WithReadiness(func() bool { return ready }),
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		call func() error
	}{
		{name: "submit", call: func() error {
			_, err := server.SubmitTask(context.Background(), nil)
			return err
		}},
		{name: "status", call: func() error {
			_, err := server.GetTaskStatus(context.Background(), nil)
			return err
		}},
		{name: "get", call: func() error {
			_, err := server.GetTask(context.Background(), nil)
			return err
		}},
		{name: "list", call: func() error {
			_, err := server.ListTasks(context.Background(), nil)
			return err
		}},
		{name: "executions", call: func() error {
			_, err := server.GetTaskExecutions(context.Background(), nil)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if code := status.Code(test.call()); code != codes.Unavailable {
				t.Fatalf("readiness gate code = %v, want %v", code, codes.Unavailable)
			}
		})
	}
	ready = true
	if _, err := server.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ready SubmitTask() code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestWithReadinessRejectsNilProvider(t *testing.T) {
	if _, err := NewCoreServer(&recordingTaskSubmitter{}, WithReadiness(nil)); err != ErrInvalidCoreReadiness {
		t.Fatalf("NewCoreServer() error = %v, want %v", err, ErrInvalidCoreReadiness)
	}
}
