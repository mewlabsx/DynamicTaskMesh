//go:build dtm_test_barrier

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"dtm/internal/agent"
	"dtm/internal/model"
)

const (
	processTestBarrierEnteredEnv = "DTM_TEST_BARRIER_ENTERED_FILE"
	processTestBarrierReleaseEnv = "DTM_TEST_BARRIER_RELEASE_FILE"
)

type processTestBarrierHandler struct {
	inner       agent.Handler
	enteredPath string
	releasePath string
}

func wrapHandlersForProcessTest(handlers []agent.Handler) ([]agent.Handler, error) {
	enteredPath := strings.TrimSpace(os.Getenv(processTestBarrierEnteredEnv))
	releasePath := strings.TrimSpace(os.Getenv(processTestBarrierReleaseEnv))
	if enteredPath == "" && releasePath == "" {
		return handlers, nil
	}
	if enteredPath == "" || releasePath == "" {
		return nil, fmt.Errorf("both %s and %s are required", processTestBarrierEnteredEnv, processTestBarrierReleaseEnv)
	}
	wrapped := append([]agent.Handler(nil), handlers...)
	for index := range wrapped {
		if wrapped[index].Capability() == model.Capability("temperature_sensor") {
			wrapped[index] = &processTestBarrierHandler{inner: wrapped[index], enteredPath: enteredPath, releasePath: releasePath}
		}
	}
	return wrapped, nil
}

func (handler *processTestBarrierHandler) Capability() model.Capability {
	return handler.inner.Capability()
}

func (handler *processTestBarrierHandler) Execute(ctx context.Context, inputs map[string]string) (map[string]any, error) {
	if err := os.WriteFile(handler.enteredPath, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0o600); err != nil {
		return nil, fmt.Errorf("write process test barrier signal: %w", err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(handler.releasePath); err == nil {
			return handler.inner.Execute(ctx, inputs)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("inspect process test barrier release: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
