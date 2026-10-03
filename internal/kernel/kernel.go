package kernel

import (
	"sync"

	"dtm/internal/kernel/capability"
	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

type ResourceID = identity.ResourceID
type CapabilityDeclarationID = identity.CapabilityDeclarationID
type CapabilityInstanceID = identity.CapabilityInstanceID
type CapabilityHandleID = identity.CapabilityHandleID
type ExecutionContextID = identity.ExecutionContextID
type EventID = identity.EventID
type ObjectKind = identity.ObjectKind
type ObjectReference = identity.ObjectReference
type LifecycleState = identity.LifecycleState
type AvailabilityState = identity.AvailabilityState
type EvaluationState = identity.EvaluationState
type OwnershipFence = resource.OwnershipFence
type ExecutionResolutionAuthority = resource.ExecutionResolutionAuthority

type Resource = resource.Resource
type CapabilityDeclaration = capability.CapabilityDeclaration
type CapabilityInstance = capability.CapabilityInstance
type CapabilityHandle = capability.CapabilityHandle
type ExecutionContext = execution.ExecutionContext
type EventRecord = event.EventRecord
type CurrentOccupancy = execution.CurrentOccupancy
type ResolutionEvidence = execution.ResolutionEvidence
type ExecutionOccupancyResolutionRequest = execution.ExecutionOccupancyResolutionRequest
type ExecutionResolutionRequest = execution.ExecutionResolutionRequest
type ResolutionStatus = execution.ResolutionStatus
type ExecutionResolutionResult = execution.ExecutionResolutionResult
type ExecutionOccupancyResolutionResult = execution.ExecutionOccupancyResolutionResult

// Kernel 暴露的 Manager 仅用于进程内 Kernel 组合与测试，不构成 DTM User Space ABI。
// User Space ABI 必须等 K1 Syscall/边界契约单独冻结后再定义。
type Kernel struct {
	Resources    *resource.Manager
	Capabilities *capability.Manager
	Executions   *execution.Manager
	Events       *event.Manager

	ResourceManager   *resource.Manager
	CapabilityManager *capability.Manager
	ExecutionManager  *execution.Manager
	EventManager      *event.Manager

	providerMu sync.RWMutex
	provider   capability.CapabilityProvider
	ownership  *executionOwnershipManager
}

func New(providers ...capability.CapabilityProvider) *Kernel {
	events := event.NewManager()
	resources := resource.NewManager(events)
	executions := execution.NewManager(events)
	capabilities := capability.NewManager(resources, executions, events)
	kernel := &Kernel{
		Resources:         resources,
		Capabilities:      capabilities,
		Executions:        executions,
		Events:            events,
		ResourceManager:   resources,
		CapabilityManager: capabilities,
		ExecutionManager:  executions,
		EventManager:      events,
	}
	kernel.ownership = newExecutionOwnershipManager(resources, capabilities, executions, events)
	if len(providers) > 0 && !isNilCapabilityProvider(providers[0]) {
		kernel.provider = providers[0]
	}
	return kernel
}

func NewKernel(providers ...capability.CapabilityProvider) *Kernel {
	return New(providers...)
}
