package mapper

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/planner"
)

var (
	ErrCapabilityUnavailable = errors.New("capability unavailable")
	ErrInvalidPlan           = errors.New("invalid plan")
)

type Discovery interface {
	Discover(model.Capability) []node.Node
}

type Eligibility interface {
	Eligible(model.NodeID) bool
}

type MappedStep struct {
	ID              model.StepID
	Capability      model.Capability
	IdempotencyMode model.IdempotencyMode
	AttemptOffset   uint32
	MaxAttempts     uint32
	NodeID          model.NodeID
	ResourceRef     model.ResourceRef
	Inputs          map[string]string
}

type MappedPlan struct {
	TaskID model.TaskID
	Steps  []MappedStep
}

type Mapper struct {
	discovery          Discovery
	eligibility        Eligibility
	resourceCandidates ResourceCandidateSource
}

type Option func(*Mapper) error

func WithEligibility(eligibility Eligibility) Option {
	return func(mapper *Mapper) error {
		if eligibility == nil || isNilEligibility(eligibility) {
			return ErrInvalidPlan
		}
		mapper.eligibility = eligibility
		return nil
	}
}

// WithResourceCandidateSource switches the Mapper into Resource Scheduling
// Mode. The source must be non-nil (including typed-nil); construction fails
// otherwise, so a Mapper that owns a source can never invoke it on a nil
// dependency. Without this option the Mapper stays in Legacy Scheduling Mode.
func WithResourceCandidateSource(source ResourceCandidateSource) Option {
	return func(mapper *Mapper) error {
		if source == nil || isNilResourceCandidateSource(source) {
			return ErrInvalidPlan
		}
		mapper.resourceCandidates = source
		return nil
	}
}

func New(discovery Discovery, options ...Option) (*Mapper, error) {
	if discovery == nil || isNilDiscovery(discovery) {
		return nil, ErrInvalidPlan
	}
	mapper := &Mapper{discovery: discovery}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidPlan
		}
		if err := option(mapper); err != nil {
			return nil, err
		}
	}
	return mapper, nil
}

func (mapper *Mapper) Map(plan planner.Plan) (MappedPlan, error) {
	if err := validatePlan(plan); err != nil {
		return MappedPlan{}, err
	}
	if mapper.resourceCandidates != nil {
		return mapper.mapResources(plan)
	}
	return mapper.mapLegacy(plan)
}

func (mapper *Mapper) mapLegacy(plan planner.Plan) (MappedPlan, error) {
	steps := make([]MappedStep, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		candidates := mapper.eligibleCandidates(
			mapper.discovery.Discover(step.Capability),
		)
		if len(candidates) == 0 {
			return MappedPlan{}, fmt.Errorf(
				"capability %q: %w",
				step.Capability,
				ErrCapabilityUnavailable,
			)
		}

		selected := selectNode(candidates)
		steps = append(steps, MappedStep{
			ID:              step.ID,
			Capability:      step.Capability,
			IdempotencyMode: step.IdempotencyMode,
			NodeID:          selected,
			Inputs:          copyInputs(step.Inputs),
		})
	}

	return MappedPlan{TaskID: plan.TaskID, Steps: steps}, nil
}

// Remap selects another currently eligible node for an already mapped step.
// excluded contains nodes that failed earlier in the same execution.
// In Resource Scheduling Mode the same exclusion signal is forwarded to the
// Candidate Source as ExcludedNodeIDs and the ResourceRef is replaced by a
// complete new mapping.
func (mapper *Mapper) Remap(step MappedStep, excluded map[model.NodeID]struct{}) (MappedStep, error) {
	if strings.TrimSpace(string(step.ID)) == "" ||
		strings.TrimSpace(string(step.Capability)) == "" {
		return MappedStep{}, ErrInvalidPlan
	}
	if mapper.resourceCandidates != nil {
		return mapper.remapResources(step, excluded)
	}
	return mapper.remapLegacy(step, excluded)
}

func (mapper *Mapper) remapLegacy(step MappedStep, excluded map[model.NodeID]struct{}) (MappedStep, error) {
	candidates := mapper.eligibleCandidates(mapper.discovery.Discover(step.Capability))
	alternatives := candidates[:0]
	for _, candidate := range candidates {
		if _, skip := excluded[candidate.ID()]; !skip {
			alternatives = append(alternatives, candidate)
		}
	}
	if len(alternatives) == 0 {
		return MappedStep{}, fmt.Errorf(
			"capability %q: %w",
			step.Capability,
			ErrCapabilityUnavailable,
		)
	}
	remapped := step
	remapped.NodeID = selectNode(alternatives)
	remapped.Inputs = copyInputs(step.Inputs)
	return remapped, nil
}

func selectNode(candidates []node.Node) model.NodeID {
	selected := candidates[0].ID()
	for _, candidate := range candidates[1:] {
		if candidate.ID() < selected {
			selected = candidate.ID()
		}
	}
	return selected
}

func (mapper *Mapper) eligibleCandidates(candidates []node.Node) []node.Node {
	eligible := make([]node.Node, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Status() != node.StatusActive {
			continue
		}
		if mapper.eligibility != nil && !mapper.eligibility.Eligible(candidate.ID()) {
			continue
		}
		eligible = append(eligible, candidate)
	}
	return eligible
}

func validatePlan(plan planner.Plan) error {
	if strings.TrimSpace(string(plan.TaskID)) == "" || len(plan.Steps) == 0 {
		return ErrInvalidPlan
	}

	seen := make(map[model.StepID]struct{}, len(plan.Steps))
	for _, step := range plan.Steps {
		if strings.TrimSpace(string(step.ID)) == "" ||
			strings.TrimSpace(string(step.Capability)) == "" {
			return ErrInvalidPlan
		}
		if _, exists := seen[step.ID]; exists {
			return ErrInvalidPlan
		}
		for key := range step.Inputs {
			if strings.TrimSpace(key) == "" {
				return ErrInvalidPlan
			}
		}
		seen[step.ID] = struct{}{}
	}

	return nil
}

func copyInputs(inputs map[string]string) map[string]string {
	if inputs == nil {
		return nil
	}

	result := make(map[string]string, len(inputs))
	for key, value := range inputs {
		result[key] = value
	}
	return result
}

func isNilDiscovery(discovery Discovery) bool {
	value := reflect.ValueOf(discovery)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilEligibility(eligibility Eligibility) bool {
	value := reflect.ValueOf(eligibility)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
