//go:build !dtm_test_barrier

package main

import "dtm/internal/agent"

func wrapHandlersForProcessTest(handlers []agent.Handler) ([]agent.Handler, error) {
	return handlers, nil
}
