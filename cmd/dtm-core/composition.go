package main

import (
	"context"
	"log"
	"time"

	"dtm/internal/config"
	"dtm/internal/runtimehost"
)

type dependencies struct{ *runtimehost.CoreCapability }

func compose() (dependencies, error) {
	return composeWithLeaseTTL(10 * time.Second)
}

func composeWithLeaseTTL(leaseTTL time.Duration) (dependencies, error) {
	capability, err := runtimehost.NewCoreCapability(context.Background(), config.Core{
		Lease: config.Lease{
			TTL:           config.Duration{Duration: leaseTTL},
			SweepInterval: config.Duration{Duration: time.Second},
		},
		Storage: config.Storage{Path: ":memory:"},
		Retry: config.Retry{
			MaxAttempts: 3,
			Backoff:     config.Duration{Duration: 100 * time.Millisecond},
		},
	}, runtimehost.CoreOptions{TaskServiceOptions: processSubmissionBarrierOptions()})
	if err != nil {
		return dependencies{}, err
	}
	return dependencies{CoreCapability: capability}, nil
}

func composeWithConfig(ctx context.Context, coreConfig config.Core) (dependencies, error) {
	capability, err := runtimehost.NewCoreCapability(ctx, coreConfig, runtimehost.CoreOptions{
		TaskServiceOptions: processSubmissionBarrierOptions(),
	})
	if err != nil {
		return dependencies{}, err
	}
	return dependencies{CoreCapability: capability}, nil
}

func composeWithConfigAndLogger(ctx context.Context, coreConfig config.Core, logger *log.Logger) (dependencies, error) {
	capability, err := runtimehost.NewCoreCapability(ctx, coreConfig, runtimehost.CoreOptions{
		Logger: logger, TaskServiceOptions: processSubmissionBarrierOptions(),
	})
	if err != nil {
		return dependencies{}, err
	}
	return dependencies{CoreCapability: capability}, nil
}
