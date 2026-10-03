package main

// restoredResourceReadiness is the production ResourceCandidateReadiness.
//
// Proof of readiness (M4-B): composeDependencies performs, in order,
//
//	repository.LoadAllResourceRecords
//	→ resourceDirectory.RestoreRecords
//	→ per-owner lifecycle restore (UpdateNodeLifecycle)
//	→ ... → RegistryServer → mapper.New
//
// all synchronously on the composition goroutine before the gRPC server is
// constructed and any task submission can reach the Mapper. The Candidate
// Source and this provider are constructed only after RestoreRecords and the
// lifecycle restore have returned successfully, and the Mapper (the only
// consumer) is constructed afterwards. Therefore every possible invocation of
// ResourceCandidateSource.Query happens after the authoritative Resource
// state has finished loading, so Ready() may be constant true.
//
// Ready=true means "the authoritative Resource state is loaded and query
// results can be interpreted authoritatively". It does NOT mean that any
// eligible candidate exists, that a Node is online, or that Lease/Endpoint
// are valid; after a Core restart restored Resources are ineligible until the
// agent re-registers, so Query returns ErrResourceUnavailable (never
// ErrResourceCandidateSourceNotReady).
type restoredResourceReadiness struct{}

func (restoredResourceReadiness) Ready() bool {
	return true
}
