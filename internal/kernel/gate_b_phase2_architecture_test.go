package kernel_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGateBPhase2ArchitectureContract(t *testing.T) {
	root := filepath.Join("..", "..")
	baseline := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "baseline.yaml"))
	concepts := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "concepts.yaml"))
	decisions := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "decisions.yaml"))
	boundaries := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "boundaries.yaml"))
	invariants := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "invariants.yaml"))
	milestone := r01MapField(t, baseline, "milestone")
	if r01StringField(t, milestone, "id") != "GATE-B-PHASE-2" || r01StringField(t, milestone, "status") != "CLOSED" || r01StringField(t, milestone, "design_status") != "ACCEPTED" || r01StringField(t, milestone, "review_status") != "PASS" || r01StringField(t, milestone, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, milestone, "implementation_readiness") != "CLOSED" || r01StringField(t, milestone, "implementation_review_status") != "PASS" || r01StringField(t, milestone, "milestone_closed") != "CLOSED" {
		t.Fatalf("milestone = %#v, want Gate B Phase 2 CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS/CLOSED", milestone)
	}
	reviewDisposition := r01MapField(t, milestone, "review_disposition")
	if r01StringField(t, reviewDisposition, "result") != "PASS" || r01StringField(t, reviewDisposition, "F-01") != "CLOSED" || r01StringField(t, reviewDisposition, "F-02") != "CLOSED" || r01StringField(t, reviewDisposition, "new_blocking_findings") != "NONE" || r01StringField(t, reviewDisposition, "runtime_scope_drift") != "NONE" {
		t.Fatalf("review_disposition = %#v, want PASS/CLOSED/CLOSED/NONE/NONE", reviewDisposition)
	}

	currentPhase := r01MapField(t, baseline, "current_phase")
	if r01StringField(t, currentPhase, "id") != "GATE-B-PHASE-2" || r01StringField(t, currentPhase, "status") != "CLOSED" || r01StringField(t, currentPhase, "design_status") != "ACCEPTED" || r01StringField(t, currentPhase, "review_status") != "PASS" || r01StringField(t, currentPhase, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, currentPhase, "implementation_readiness") != "CLOSED" || r01StringField(t, currentPhase, "implementation_review_status") != "PASS" || r01StringField(t, currentPhase, "decision") != "GATE_B_PHASE_2_CLOSED" {
		t.Fatalf("current_phase = %#v, want Gate B Phase 2 accepted design and bounded readiness", currentPhase)
	}
	currentSubphase := r01MapField(t, currentPhase, "current_subphase")
	if r01StringField(t, currentSubphase, "id") != "EXECUTION-RESOLUTION-MECHANISM" || r01StringField(t, currentSubphase, "status") != "CLOSED" || r01StringField(t, currentSubphase, "design_status") != "ACCEPTED" || r01StringField(t, currentSubphase, "review_status") != "PASS" || r01StringField(t, currentSubphase, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, currentSubphase, "implementation_readiness") != "CLOSED" || r01StringField(t, currentSubphase, "implementation_review_status") != "PASS" {
		t.Fatalf("current_subphase = %#v, want execution-resolution CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS", currentSubphase)
	}
	if r01StringField(t, r01MapField(t, baseline, "state"), "current_review") != "DTM-Kernel-v0.1-Gate-B-Phase-2-Closure-Record" {
		t.Fatal("Architecture State current_review must point to the Phase 2 closure record")
	}
	transition := r01MapField(t, baseline, "governance_transition")
	if r01StringField(t, transition, "prior_candidate") != "DESIGN_ONLY / REVIEW_PENDING / NOT_IMPLEMENTED" || r01StringField(t, transition, "transition") != "FRESH_INDEPENDENT_REVIEW = PASS" || r01StringField(t, transition, "accepted_state") != "DESIGN_ACCEPTED / READY_FOR_BOUNDED_IMPLEMENTATION / NOT_IMPLEMENTED" || r01StringField(t, transition, "implementation_transition") != "BOUNDED_IMPLEMENTATION = COMPLETE" || r01StringField(t, transition, "implementation_state") != "BOUNDED_IMPLEMENTED / READY_FOR_INDEPENDENT_REVIEW / REVIEW_PENDING" {
		t.Fatalf("governance_transition = %#v, want preserved pre-review chronology", transition)
	}

	phase1 := r01EntryByID(t, baseline, "milestone_history", "id", "GATE-B-PHASE-1")
	if r01StringField(t, phase1, "status") != "CLOSED" || r01StringField(t, phase1, "implementation_status") != "BOUNDED_IMPLEMENTED" {
		t.Fatalf("Gate B Phase 1 history = %#v, want CLOSED/BOUNDED_IMPLEMENTED", phase1)
	}
	semanticContracts := r01MapField(t, r01MapField(t, baseline, "freeze"), "semantic_contracts")
	if r01StringField(t, semanticContracts, "GATE_B_PHASE_1") != "CLOSED" || r01StringField(t, semanticContracts, "GATE_B_PHASE_2") != "CLOSED" {
		t.Fatalf("semantic contracts = %#v, want Phase 1 and Phase 2 CLOSED", semanticContracts)
	}

	successCriteria := r01MapField(t, baseline, "success_criteria")
	phase2Gate := r01EntryByID(t, successCriteria, "gates", "id", "GATE-B-PHASE-2-EXECUTION-RESOLUTION")
	if r01StringField(t, phase2Gate, "status") != "CLOSED" || r01StringField(t, phase2Gate, "design_status") != "ACCEPTED" || r01StringField(t, phase2Gate, "review_status") != "PASS" || r01StringField(t, phase2Gate, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, phase2Gate, "implementation_readiness") != "CLOSED" || r01StringField(t, phase2Gate, "implementation_review_status") != "PASS" {
		t.Fatalf("Phase 2 gate = %#v, want CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS", phase2Gate)
	}
	fullGateB := r01EntryByID(t, successCriteria, "gates", "id", "GATE-B-AUTONOMOUS-KERNEL-LOOP")
	if r01StringField(t, fullGateB, "status") != "RESERVED" || r01StringField(t, fullGateB, "implementation_status") != "PHASE_2_BOUNDED_IMPLEMENTED" {
		t.Fatalf("full Gate B = %#v, want RESERVED/PHASE_2_BOUNDED_IMPLEMENTED", fullGateB)
	}
	fullGateInterpretation := r01StringField(t, fullGateB, "interpretation")
	for _, required := range []string{
		"Gate B Phase 1 is closed",
		"Gate B Phase 2 design is accepted",
		"bounded same-fence execution-resolution runtime implementation is accepted and closed",
		"Full Gate B autonomous-loop completion remains RESERVED",
	} {
		if !strings.Contains(fullGateInterpretation, required) {
			t.Fatalf("full Gate B interpretation missing %q: %q", required, fullGateInterpretation)
		}
	}
	checkpoint := r01MapField(t, baseline, "checkpoint")
	if r01StringField(t, checkpoint, "final_architecture_freeze") != "NOT_CLAIMED" {
		t.Fatalf("checkpoint.final_architecture_freeze = %q, want NOT_CLAIMED", r01StringField(t, checkpoint, "final_architecture_freeze"))
	}

	task := r01EntryByID(t, concepts, "concepts", "ConceptID", "Task")
	if r01StringField(t, task, "owner") != "User Space / Policy Layer" || r01StringField(t, task, "status") != "FROZEN_SEMANTICS_ONLY" || r01StringField(t, task, "semantic_role") != "external_policy_managed_concept" {
		t.Fatalf("Task concept = %#v, want external User Space/Policy Layer ownership", task)
	}
	if !strings.Contains(r01StringField(t, task, "definition"), "Kernel-external") || !strings.Contains(r01StringField(t, task, "kernel_visibility"), "not a Kernel Object") {
		t.Fatalf("Task concept does not preserve the non-Kernel-Object boundary: %#v", task)
	}
	taskDecision := r01EntryByID(t, decisions, "decisions", "id", "D-TASK-EXTERNAL-POLICY-MANAGEMENT")
	if r01StringField(t, taskDecision, "status") != "FROZEN_SEMANTICS_ONLY" || !strings.Contains(r01StringField(t, taskDecision, "lineage_rule"), "at most one Capability invocation") {
		t.Fatalf("Task decision = %#v, want external ownership and lineage cardinality", taskDecision)
	}

	resolutionEvidence := r01EntryByID(t, concepts, "concepts", "ConceptID", "ResolutionEvidence")
	if isObject, ok := resolutionEvidence["kernel_object"].(bool); !ok || isObject {
		t.Fatalf("ResolutionEvidence kernel_object = %#v, want false", resolutionEvidence["kernel_object"])
	}
	for _, conceptID := range []string{"CurrentOccupancy", "OccupancyResolutionAuthority", "OwnershipFence"} {
		concept := r01EntryByID(t, concepts, "concepts", "ConceptID", conceptID)
		if r01StringField(t, concept, "status") != "ACCEPTED" || r01StringField(t, concept, "design_status") != "ACCEPTED" || r01StringField(t, concept, "review_status") != "PASS" || r01StringField(t, concept, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, concept, "implementation_readiness") != "CLOSED" || r01StringField(t, concept, "implementation_review_status") != "PASS" {
			t.Fatalf("%s = %#v, want accepted design with closed bounded implementation and PASS review", conceptID, concept)
		}
		if isObject, ok := concept["kernel_object"].(bool); !ok || isObject {
			t.Fatalf("%s kernel_object = %#v, want false", conceptID, concept["kernel_object"])
		}
	}
	executionObservation := r01EntryByID(t, concepts, "concepts", "ConceptID", "ExecutionObservation")
	if !strings.Contains(r01StringField(t, executionObservation, "definition"), "immutable historical") || !strings.Contains(r01StringField(t, executionObservation, "definition"), "later resolution cannot rewrite") {
		t.Fatalf("ExecutionObservation concept does not preserve historical immutability: %#v", executionObservation)
	}

	phase2Decision := r01EntryByID(t, decisions, "decisions", "id", "D-GATE-B-PHASE-2-EXECUTION-RESOLUTION")
	if r01StringField(t, phase2Decision, "status") != "ACCEPTED" || r01StringField(t, phase2Decision, "design_status") != "ACCEPTED" || r01StringField(t, phase2Decision, "review_status") != "PASS" || r01StringField(t, phase2Decision, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, phase2Decision, "implementation_readiness") != "CLOSED" || r01StringField(t, phase2Decision, "implementation_review_status") != "PASS" {
		t.Fatalf("Phase 2 decision = %#v, want ACCEPTED/design PASS/BOUNDED_IMPLEMENTED/CLOSED/PASS", phase2Decision)
	}
	if r01StringField(t, phase2Decision, "supersession_mode") != "PARTIAL" {
		t.Fatalf("Phase 2 supersession_mode = %q, want PARTIAL", r01StringField(t, phase2Decision, "supersession_mode"))
	}
	idempotencyRule := r01StringField(t, phase2Decision, "idempotency_rule")
	for _, required := range []string{
		"immutable original ExecutionObservation",
		"Occupancy UNKNOWN",
		"RELEASED with CurrentOccupancy ENDED",
		"exact Resource claim is absent",
		"terminal state alone is not resolution history",
	} {
		if !strings.Contains(idempotencyRule, required) {
			t.Fatalf("Phase 2 idempotency_rule missing %q: %q", required, idempotencyRule)
		}
	}
	if _, hasRefines := phase2Decision["refines"]; hasRefines {
		t.Fatal("Phase 2 must not rely on an unresolved refines edge")
	}
	if supersedes := r01ListField(t, phase2Decision, "supersedes"); len(supersedes) != 0 {
		t.Fatalf("Phase 2 must use scoped partial supersession, not direct supersedes: %#v", supersedes)
	}
	partial := r01EntryByID(t, phase2Decision, "partially_supersedes", "id", "D-GATE-B-PHASE-1-EXECUTION-OWNERSHIP")
	supersededScope := r01StringField(t, partial, "scope")
	if !strings.Contains(supersededScope, "post-Provider") || !strings.Contains(supersededScope, "authoritative ENDED") || !strings.Contains(supersededScope, "exact execution-claim release responsibility") {
		t.Fatalf("Phase 2 superseded scope = %q, want only post-Provider ENDED release responsibility", supersededScope)
	}
	retainedAspect := r01StringField(t, partial, "retained_aspect")
	for _, retained := range []string{
		"Resource-owned execution capacity",
		"authority-before-allocation",
		"immutable ExecutionDescriptor / ExecutionAllocation binding",
		"exact allocation claim ownership",
		"single controlled Dispatch claim",
		"Dispatch-time Provider resolution",
		"UNKNOWN-safe claim retention",
		"no blind retry",
		"Resource availability separation",
		"all other accepted Gate B Phase 1 ownership semantics",
	} {
		if !strings.Contains(retainedAspect, retained) {
			t.Fatalf("Phase 2 retained_aspect missing %q: %q", retained, retainedAspect)
		}
	}
	if effectiveRule := r01StringField(t, phase2Decision, "effective_state_rule"); !strings.Contains(effectiveRule, "PARTIAL") || !strings.Contains(effectiveRule, "retained Phase 1 aspects remain effective") {
		t.Fatalf("Phase 2 effective_state_rule = %q, want scoped retained semantics", effectiveRule)
	}
	reviewFix := r01MapField(t, phase2Decision, "review_fix")
	if r01StringField(t, reviewFix, "F-01") != "FIXED" || r01StringField(t, reviewFix, "F-02") != "FIXED_BY_BOUNDED_SCOPE" {
		t.Fatalf("Phase 2 review_fix = %#v, want F-01 FIXED and F-02 FIXED_BY_BOUNDED_SCOPE", reviewFix)
	}
	sameFenceRule := r01StringField(t, phase2Decision, "same_fence_rule")
	if !strings.Contains(sameFenceRule, "SAME-FENCE OCCUPANCY RESOLUTION ONLY") || !strings.Contains(sameFenceRule, "ResolutionAuthorityFence") || !strings.Contains(sameFenceRule, "ExecutionAllocation.OwnershipFence") {
		t.Fatalf("Phase 2 same_fence_rule = %q, want exact same-fence binding", sameFenceRule)
	}
	crossFenceRule := r01StringField(t, phase2Decision, "cross_fence_rule")
	for _, required := range []string{"E17", "E18", "no retrospective authority", "UNKNOWN", "HELD", "RESERVED"} {
		if !strings.Contains(crossFenceRule, required) {
			t.Fatalf("Phase 2 cross_fence_rule missing %q: %q", required, crossFenceRule)
		}
	}
	if transitionScope := r01StringField(t, phase2Decision, "fence_transition_scope"); !strings.Contains(transitionScope, "cross-fence UNKNOWN resolution") || !strings.Contains(transitionScope, "ownership authority migration") {
		t.Fatalf("Phase 2 fence_transition_scope = %q, want transition and cross-fence exclusions", transitionScope)
	}
	if strings.Contains(readR01ArchitectureFile(t, filepath.Join(root, "architecture", "decisions.yaml")), "\n    refines:") {
		t.Fatal("architecture decisions must not retain refines as a competing effective-state mechanism")
	}
	crossFence := r01EntryByID(t, baseline, "reserved_future_mechanisms", "id", "CrossFenceResolution")
	if r01StringField(t, crossFence, "status") != "RESERVED" {
		t.Fatalf("CrossFenceResolution = %#v, want RESERVED", crossFence)
	}
	fenceInvariant := r01EntryByID(t, invariants, "invariants", "id", "INV-OWNERSHIP-FENCE-NOT-TERMINATION-PROOF")
	if fenceRule := r01StringField(t, fenceInvariant, "rule"); !strings.Contains(fenceRule, "advancement alone does not prove") || !strings.Contains(fenceRule, "future") {
		t.Fatalf("ownership-fence invariant = %q, want advancement not to prove termination", fenceRule)
	}
	resolutionBoundary := r01EntryByID(t, boundaries, "boundaries", "layer", "Execution Resolution")
	if r01StringField(t, resolutionBoundary, "status") != "ACCEPTED" || r01StringField(t, resolutionBoundary, "design_status") != "ACCEPTED" || r01StringField(t, resolutionBoundary, "review_status") != "PASS" || r01StringField(t, resolutionBoundary, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, resolutionBoundary, "implementation_readiness") != "CLOSED" || r01StringField(t, resolutionBoundary, "implementation_review_status") != "PASS" {
		t.Fatalf("Execution Resolution boundary = %#v, want ACCEPTED/design PASS/BOUNDED_IMPLEMENTED/CLOSED/PASS", resolutionBoundary)
	}

	for _, invariantID := range []string{
		"INV-TASK-EXTERNAL-POLICY-MANAGED",
		"INV-RESOLUTION-EVIDENCE-NOT-KERNEL-OBJECT",
		"INV-EXECUTION-OBSERVATION-IMMUTABLE",
		"INV-CURRENT-OCCUPANCY-AUTHORITY",
		"INV-UNKNOWN-OCCUPANCY-RETAINS-CLAIM",
		"INV-ENDED-OCCUPANCY-RELEASES-CLAIM",
		"INV-RELEASED-IMPLIES-ENDED",
		"INV-RESOLUTION-AUTHORITY-SEPARATION",
		"INV-RESOLUTION-EXACT-ALLOCATION-BINDING",
		"INV-RESOLUTION-FENCE-AUTHORITY",
		"INV-RESOLUTION-SAME-FENCE-ONLY",
		"INV-CROSS-FENCE-UNKNOWN-FAIL-CLOSED",
		"INV-OWNERSHIP-FENCE-NOT-TERMINATION-PROOF",
		"INV-RESOLUTION-ATOMIC-RELEASE",
		"INV-RESOLUTION-IDEMPOTENT",
		"INV-RESOLUTION-RESOURCE-AVAILABILITY-SEPARATION",
		"INV-PHASE-2-IN-PROCESS-ONLY",
		"INV-CANCELLATION-IS-NOT-ENDED",
	} {
		invariant := r01EntryByID(t, invariants, "invariants", "id", invariantID)
		if invariantID != "INV-TASK-EXTERNAL-POLICY-MANAGED" && (r01StringField(t, invariant, "status") != "ACCEPTED" || r01StringField(t, invariant, "design_status") != "ACCEPTED" || r01StringField(t, invariant, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, invariant, "implementation_readiness") != "CLOSED" || r01StringField(t, invariant, "implementation_review_status") != "PASS") {
			t.Fatalf("%s = %#v, want accepted design with closed bounded implementation and PASS review", invariantID, invariant)
		}
		if invariantID != "INV-TASK-EXTERNAL-POLICY-MANAGED" && r01StringField(t, invariant, "review_status") != "PASS" {
			t.Fatalf("%s review_status = %q, want PASS", invariantID, r01StringField(t, invariant, "review_status"))
		}
		if invariantID == "INV-RESOLUTION-IDEMPOTENT" {
			idempotencyInvariant := r01StringField(t, invariant, "rule")
			for _, required := range []string{"immutable original ExecutionObservation", "Occupancy UNKNOWN", "exact Resource claim is NONE", "terminal state alone is not resolution history"} {
				if !strings.Contains(idempotencyInvariant, required) {
					t.Fatalf("INV-RESOLUTION-IDEMPOTENT rule missing %q: %q", required, idempotencyInvariant)
				}
			}
		}
	}

	assertCurrentDesignText(t, root, "execution-contract.md", []string{
		"ResolutionEvidence is not a Kernel Object",
		"CurrentOccupancy",
		"OwnershipFence",
		"NOT_ESTABLISHED",
		"UNKNOWN retains the exact claim",
		"Cancellation request != execution ended",
		"ResolutionAuthorityFence must equal ExecutionAllocation.OwnershipFence",
		"E18 authority cannot resolve an E17 allocation",
		"Cross-fence resolution",
		"fence-transition runtime",
		"ALREADY_RESOLVED requires the original immutable UNKNOWN observation",
		"Terminal state alone is not resolution history",
		"exactly one accepted immutable resolution EventRecord",
		"must not partially change the claim/state/event boundary",
	})
	assertCurrentDesignText(t, root, "task-model.md", []string{
		"not Kernel Objects",
		"RootTaskRef and ChildTaskRef are opaque lineage",
		"at most one Capability invocation",
		"Child success alone does not prove Root goal satisfaction",
	})
	for _, idAndStatus := range []struct {
		id     string
		status string
	}{
		{id: "K2-OPEN-RESOURCE-ADMISSION-BRIDGE", status: "OPEN"},
		{id: "K1-OPEN-CROSS-MANAGER-CONSISTENCY", status: "OPEN"},
		{id: "K1-DEFERRED-SYSCALL-ABI", status: "DEFERRED"},
		{id: "K1-DEFERRED-SCOPE-GOVERNANCE", status: "DEFERRED"},
	} {
		decision := r01EntryByID(t, decisions, "decisions", "id", idAndStatus.id)
		if r01StringField(t, decision, "status") != idAndStatus.status {
			t.Fatalf("decision %s status = %q, want %q", idAndStatus.id, r01StringField(t, decision, "status"), idAndStatus.status)
		}
	}
	if !strings.Contains(readR01ArchitectureFile(t, filepath.Join(root, "architecture", "decisions.yaml")), "D-R0-DEFERRAL-NOT-NEGATION") {
		t.Fatal("Phase 2 must preserve explicit DEFERRED/RESERVED status semantics")
	}

	assertPhase2ProductionSourcePreservesImplementationBoundary(t, root)
}

func assertPhase2ProductionSourcePreservesImplementationBoundary(t *testing.T, root string) {
	t.Helper()
	implementationPath := filepath.Join(root, "internal", "kernel", "execution_ownership.go")
	implementationContents, err := os.ReadFile(implementationPath)
	if err != nil {
		t.Fatalf("read %s: %v", implementationPath, err)
	}
	for _, required := range []string{
		"CurrentOccupancy",
		"OwnershipFence",
		"GetExecutionResolutionAuthority",
		"ResolveExecutionOccupancy",
		"commitEndedAllocationLocked",
	} {
		if !strings.Contains(string(implementationContents), required) {
			t.Fatalf("bounded Phase 2 implementation is missing runtime symbol %q", required)
		}
	}

	for _, relative := range []string{
		filepath.Join("internal", "kernel", "execution_ownership.go"),
		filepath.Join("internal", "kernel", "local_runtime.go"),
		filepath.Join("internal", "kernel", "resource"),
		filepath.Join("internal", "kernel", "execution"),
	} {
		path := filepath.Join(root, relative)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		checkFile := func(filePath string) error {
			contents, err := os.ReadFile(filePath)
			if err != nil {
				return err
			}
			for _, forbidden := range []string{
				"ResolutionEvidenceManager",
				"ResolutionEvidenceStore",
				"ObjectKindExecutionResolution",
				"ObjectKindResolutionEvidence",
				"TaskManager",
				"Scheduler",
				"Planner",
				"CancelExecution",
				"PreemptExecution",
				"RetryExecution",
				"RemoteResolution",
				"RecoveryManager",
				"PersistenceManager",
				"AdvanceOwnershipFence",
				"FenceTransition",
			} {
				if strings.Contains(string(contents), forbidden) {
					return &phase2RuntimeSymbolError{path: filePath, symbol: forbidden}
				}
			}
			return nil
		}
		if info.IsDir() {
			err = filepath.Walk(path, func(filePath string, fileInfo os.FileInfo, walkErr error) error {
				if walkErr != nil || fileInfo.IsDir() || filepath.Ext(filePath) != ".go" || strings.HasSuffix(filePath, "_test.go") {
					return walkErr
				}
				return checkFile(filePath)
			})
		} else {
			err = checkFile(path)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

type phase2RuntimeSymbolError struct {
	path   string
	symbol string
}

func (err *phase2RuntimeSymbolError) Error() string {
	return "Phase 2 runtime symbol " + err.symbol + " found in " + err.path
}
