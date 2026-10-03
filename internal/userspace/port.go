// Package userspace contains the bounded User Space execution boundary.
//
// The package intentionally depends on a small value-oriented port instead of
// the Kernel implementation or any of its managers.  The port is an
// in-process composition boundary for US-0; it is not a physical ABI.
package userspace

import "dtm/internal/kernel"

// ExecutionKernelPort is the complete Kernel dependency of the bounded
// execution orchestrator.  Both operations return value snapshots.  The port
// deliberately does not expose allocation, Resource, Provider, or occupancy
// mutation primitives.
type ExecutionKernelPort interface {
	RequestExecution(kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error)
	ObserveInvocation(kernel.InvocationID) (kernel.LogicalInvocationObservation, error)
}

var _ ExecutionKernelPort = (*kernel.LogicalExecutionFacade)(nil)

// KernelExecutionPort is a descriptive compatibility alias for callers that
// use the name from the User-Space boundary documents.
type KernelExecutionPort = ExecutionKernelPort
