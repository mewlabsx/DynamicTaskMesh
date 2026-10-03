package mapper

import (
	"fmt"
	"reflect"
	"sort"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

// ResourceDirectoryReader is the minimal read-only Directory dependency of
// the Candidate Source. The existing resourcedirectory.Directory satisfies it
// structurally; no write or lifecycle API is exposed.
type ResourceDirectoryReader interface {
	MatchRequirement(
		requirement model.ResourceRequirement,
	) ([]resourcedirectory.ResourceRecordView, error)
}

// ResourceCandidateReadiness reports whether the authoritative Resource state
// has finished loading. It is a process-startup-level, monotonic false→true
// state; it does not track per-Node or per-candidate conditions.
type ResourceCandidateReadiness interface {
	Ready() bool
}

// ResourceCandidateSource is the frozen M4-A interface. The Ready check is
// performed inside Query; callers must not split readiness checks from
// queries.
type ResourceCandidateSource interface {
	Query(
		query ResourceCandidateQuery,
	) ([]resourcedirectory.ResourceRecordView, error)
}

type directoryResourceCandidateSource struct {
	directory ResourceDirectoryReader
	readiness ResourceCandidateReadiness
}

// NewResourceCandidateSource builds a Candidate Source over a read-only
// Directory reader and a Readiness provider. It is a test-oriented
// constructor; production composition injection belongs to M4-B.
func NewResourceCandidateSource(
	directory ResourceDirectoryReader,
	readiness ResourceCandidateReadiness,
) (ResourceCandidateSource, error) {
	if isNilDirectoryReader(directory) {
		return nil, ErrInvalidResourceCandidateQuery
	}
	if isNilReadiness(readiness) {
		return nil, ErrInvalidResourceCandidateQuery
	}
	return &directoryResourceCandidateSource{directory: directory, readiness: readiness}, nil
}

// Query implements the frozen query order: validate → Ready → Directory query
// → OwnerNode / Resource / Node filtering → ProviderNodeID → ResourceID
// sorting → empty result classification.
func (source *directoryResourceCandidateSource) Query(
	query ResourceCandidateQuery,
) ([]resourcedirectory.ResourceRecordView, error) {
	if err := query.validate(); err != nil {
		return nil, err
	}
	if !source.readiness.Ready() {
		return nil, ErrResourceCandidateSourceNotReady
	}
	views, err := source.directory.MatchRequirement(query.Requirement)
	if err != nil {
		return nil, fmt.Errorf("%w: match resource requirement: %w", ErrResourceCandidateQueryFailed, err)
	}
	excludedResources := resourceIDSet(query.ExcludedResourceIDs)
	excludedNodes := nodeIDSet(query.ExcludedNodeIDs)
	filtered := make([]resourcedirectory.ResourceRecordView, 0, len(views))
	for _, view := range views {
		if !view.Eligible {
			return nil, fmt.Errorf(
				"%w: directory returned ineligible view for resource %q",
				ErrResourceCandidateQueryFailed,
				view.Descriptor.ID,
			)
		}
		if query.OwnerNodeID != "" && view.Descriptor.OwnerNodeID != query.OwnerNodeID {
			continue
		}
		if _, exists := excludedResources[view.Descriptor.ID]; exists {
			continue
		}
		if _, exists := excludedNodes[view.Descriptor.OwnerNodeID]; exists {
			continue
		}
		filtered = append(filtered, view.Clone())
	}
	sortCandidates(filtered)
	if len(filtered) == 0 {
		return nil, resourceUnavailableError(query.Requirement)
	}
	return filtered, nil
}

// sortCandidates applies the frozen scheduling order: ProviderNodeID then
// ResourceID. It never relies on the Directory's own ordering.
func sortCandidates(views []resourcedirectory.ResourceRecordView) {
	sort.SliceStable(views, func(left, right int) bool {
		if views[left].Descriptor.OwnerNodeID != views[right].Descriptor.OwnerNodeID {
			return views[left].Descriptor.OwnerNodeID < views[right].Descriptor.OwnerNodeID
		}
		return views[left].Descriptor.ID < views[right].Descriptor.ID
	})
}

func resourceIDSet(values []model.ResourceID) map[model.ResourceID]struct{} {
	set := make(map[model.ResourceID]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func nodeIDSet(values []model.NodeID) map[model.NodeID]struct{} {
	set := make(map[model.NodeID]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func isNilDirectoryReader(reader ResourceDirectoryReader) bool {
	if reader == nil {
		return true
	}
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilReadiness(readiness ResourceCandidateReadiness) bool {
	if readiness == nil {
		return true
	}
	value := reflect.ValueOf(readiness)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
