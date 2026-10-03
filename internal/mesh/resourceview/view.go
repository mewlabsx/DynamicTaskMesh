package resourceview

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"dtm/internal/mesh/protocol"
	"dtm/internal/model"
)

const (
	MaxResourcesPerOwner = 256
	MaxOperations        = 64
	MaxAttributes        = 64
	MaxTextBytes         = 1024
)

var (
	ErrClosed                = errors.New("mesh resource view is closed")
	ErrInvalidAdvertisement  = errors.New("invalid mesh resource advertisement")
	ErrUnknownRuntimeSession = errors.New("mesh resource owner session is not known by membership")
	ErrNamespaceMismatch     = errors.New("mesh resource owner namespace mismatch")
	ErrProtocolMajorMismatch = errors.New("mesh resource owner protocol major mismatch")
	ErrDuplicateResourceID   = errors.New("duplicate mesh resource id")
)

// Descriptor is a pre-authority Resource fact. It deliberately contains no
// generation, registration, Lease, or ResourceRef.
type Descriptor struct {
	ID         model.ResourceID
	Kind       model.ResourceKind
	Type       model.ResourceType
	Operations []model.OperationID
	Attributes map[string]string
}

func ProjectLocalCapability(nodeID model.NodeID, capability model.Capability) (Descriptor, error) {
	requirement, err := model.LegacyCapabilityRequirement(capability)
	if err != nil {
		return Descriptor{}, err
	}
	resourceID, err := model.LegacyCapabilityResourceID(nodeID, requirement.Type)
	if err != nil {
		return Descriptor{}, err
	}
	result := Descriptor{ID: resourceID, Kind: requirement.Kind, Type: requirement.Type, Operations: []model.OperationID{requirement.OperationID}, Attributes: map[string]string{}}
	if err := result.Validate(); err != nil {
		return Descriptor{}, err
	}
	return result, nil
}

func (descriptor Descriptor) Validate() error {
	if descriptor.ID.Validate() != nil || descriptor.Kind.Validate() != nil || descriptor.Type.Validate() != nil || len(descriptor.Operations) == 0 || len(descriptor.Operations) > MaxOperations || len(descriptor.Attributes) > MaxAttributes || len(descriptor.ID) > MaxTextBytes || len(descriptor.Type) > MaxTextBytes {
		return ErrInvalidAdvertisement
	}
	seen := map[model.OperationID]struct{}{}
	for _, operation := range descriptor.Operations {
		if operation.Validate() != nil || len(operation) > MaxTextBytes {
			return ErrInvalidAdvertisement
		}
		if _, exists := seen[operation]; exists {
			return ErrInvalidAdvertisement
		}
		seen[operation] = struct{}{}
	}
	for key, value := range descriptor.Attributes {
		if strings.TrimSpace(key) == "" || len(key) > MaxTextBytes || len(value) > MaxTextBytes || strings.ContainsRune(key, 0) || strings.ContainsRune(value, 0) {
			return ErrInvalidAdvertisement
		}
	}
	return nil
}

func (descriptor Descriptor) Clone() Descriptor {
	clone := descriptor
	clone.Operations = append([]model.OperationID(nil), descriptor.Operations...)
	clone.Attributes = make(map[string]string, len(descriptor.Attributes))
	for key, value := range descriptor.Attributes {
		clone.Attributes[key] = value
	}
	return clone
}

type Advertisement struct {
	Owner     protocol.Identity
	Resources []Descriptor
}

type Entry struct {
	Owner      protocol.Identity
	Descriptor Descriptor
}

type Snapshot struct {
	Entries         []Entry
	ActiveResources []Entry
}

type MembershipSnapshot struct {
	Members       []protocol.Identity
	ActiveMembers []protocol.Identity
}

type MembershipSource func() MembershipSnapshot

type View struct {
	mu         sync.RWMutex
	local      protocol.Identity
	membership MembershipSource
	byOwner    map[string]Advertisement
	closed     bool
}

func New(local protocol.Identity, members MembershipSource, localResources []Descriptor) (*View, error) {
	if members == nil || local.Validate() != nil {
		return nil, ErrInvalidAdvertisement
	}
	view := &View{local: local, membership: members, byOwner: map[string]Advertisement{}}
	if err := view.observeLocked(Advertisement{Owner: local, Resources: localResources}); err != nil {
		return nil, err
	}
	return view, nil
}

func (view *View) Observe(advertisement Advertisement) error {
	view.mu.Lock()
	defer view.mu.Unlock()
	if view.closed {
		return ErrClosed
	}
	return view.observeLocked(advertisement)
}

func (view *View) observeLocked(advertisement Advertisement) error {
	if advertisement.Owner.Validate() != nil {
		return ErrInvalidAdvertisement
	}
	if advertisement.Owner.MeshNamespace != view.local.MeshNamespace {
		return ErrNamespaceMismatch
	}
	if advertisement.Owner.ProtocolMajor != view.local.ProtocolMajor {
		return ErrProtocolMajorMismatch
	}
	known := false
	for _, member := range view.membership().Members {
		if member.SameSession(advertisement.Owner) {
			known = true
			break
		}
	}
	if !known {
		return ErrUnknownRuntimeSession
	}
	if len(advertisement.Resources) > MaxResourcesPerOwner {
		return ErrInvalidAdvertisement
	}
	resources := make([]Descriptor, len(advertisement.Resources))
	seen := map[model.ResourceID]struct{}{}
	for i, descriptor := range advertisement.Resources {
		if err := descriptor.Validate(); err != nil {
			return err
		}
		if _, exists := seen[descriptor.ID]; exists {
			return ErrDuplicateResourceID
		}
		seen[descriptor.ID] = struct{}{}
		resources[i] = descriptor.Clone()
	}
	sortDescriptors(resources)
	advertisement.Resources = resources
	view.byOwner[ownerKey(advertisement.Owner)] = advertisement
	return nil
}

func (view *View) Snapshot() Snapshot {
	view.mu.RLock()
	defer view.mu.RUnlock()
	entries := make([]Entry, 0)
	for _, advertisement := range view.byOwner {
		for _, descriptor := range advertisement.Resources {
			entries = append(entries, Entry{Owner: advertisement.Owner, Descriptor: descriptor.Clone()})
		}
	}
	sortEntries(entries)
	activeSessions := map[string]struct{}{}
	for _, member := range view.membership().ActiveMembers {
		activeSessions[ownerKey(member)] = struct{}{}
	}
	active := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if _, ok := activeSessions[ownerKey(entry.Owner)]; ok {
			active = append(active, entry)
		}
	}
	return Snapshot{Entries: entries, ActiveResources: active}
}

// AdvertisementReady reports whether this local projection has observed the
// current session's complete advertisement. An observed empty advertisement
// is ready too; absence of an entry means the owner has not been observed.
func (view *View) AdvertisementReady(owner protocol.Identity) bool {
	if view == nil {
		return false
	}
	view.mu.RLock()
	defer view.mu.RUnlock()
	_, exists := view.byOwner[ownerKey(owner)]
	return exists
}

func (view *View) Close() error { view.mu.Lock(); view.closed = true; view.mu.Unlock(); return nil }
func ownerKey(identity protocol.Identity) string {
	return identity.NodeID + "\x00" + identity.RuntimeInstance
}
func sortDescriptors(values []Descriptor) {
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
}
func sortEntries(values []Entry) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].Owner.NodeID != values[j].Owner.NodeID {
			return values[i].Owner.NodeID < values[j].Owner.NodeID
		}
		if values[i].Owner.RuntimeInstance != values[j].Owner.RuntimeInstance {
			return values[i].Owner.RuntimeInstance < values[j].Owner.RuntimeInstance
		}
		return values[i].Descriptor.ID < values[j].Descriptor.ID
	})
}
