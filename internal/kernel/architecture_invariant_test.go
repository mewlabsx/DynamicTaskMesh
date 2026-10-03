package kernel_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestInvariant_DeferredStatusIsNotArchitecturalRemoval(t *testing.T) {
	root := filepath.Join("..", "..")
	architectureFiles := []string{
		filepath.Join(root, "architecture", "baseline.yaml"),
		filepath.Join(root, "architecture", "boundaries.yaml"),
		filepath.Join(root, "architecture", "concepts.yaml"),
		filepath.Join(root, "architecture", "decisions.yaml"),
		filepath.Join(root, "architecture", "invariants.yaml"),
	}
	combined := make([]string, 0, len(architectureFiles))
	for _, path := range architectureFiles {
		combined = append(combined, readR01ArchitectureFile(t, path))
	}
	contents := strings.Join(combined, "\n")
	for _, required := range []string{
		"D-R0-DEFERRAL-NOT-NEGATION",
		"DEFERRED, RESERVED, OPEN, and NEEDS_REVIEW classify future or unresolved architecture",
		"INV-DEFERRED-NOT-NEGATED",
		"INV-R0-CHECKPOINT-NOT-FINAL-FREEZE",
		"status: \"FREEZE_CANDIDATE\"",
		"final_architecture_freeze: \"NOT_CLAIMED\"",
		"implementation_status: \"NOT_IMPLEMENTED\"",
	} {
		if !strings.Contains(contents, required) {
			t.Fatalf("architecture status contract missing %q", required)
		}
	}
}

func TestInvariant_PreFreezeContractImplementationAcceptedVerified(t *testing.T) {
	root := filepath.Join("..", "..")
	baseline := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "baseline.yaml"))
	contract := r01MapField(t, baseline, "pre_freeze_contract_design")
	if r01StringField(t, contract, "id") != "DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN" ||
		r01StringField(t, contract, "status") != "ACCEPTED" ||
		r01StringField(t, contract, "design_status") != "ACCEPTED" ||
		r01StringField(t, contract, "review_status") != "PASS" ||
		r01StringField(t, contract, "implementation_status") != "BOUNDED_IMPLEMENTED" ||
		r01StringField(t, contract, "implementation_readiness") != "CLOSED" ||
		r01StringField(t, contract, "implementation_review_status") != "PASS" ||
		r01StringField(t, contract, "implementation_acceptance") != "ACCEPTED" ||
		r01StringField(t, contract, "implementation_acceptance_verification") != "PASS" ||
		r01StringField(t, contract, "current_review") != "DTM-Kernel-Pre-Freeze-Logical-Execution-Facade-Implementation-Acceptance" ||
		r01StringField(t, contract, "implementation_fix_document") != "docs/status.md#recorded-status" ||
		r01StringField(t, contract, "implementation_acceptance_document") != "docs/status.md#recorded-status" ||
		r01StringField(t, contract, "implementation_fix_status") != "ACCEPTED" ||
		r01StringField(t, contract, "runtime_changes") != "BOUNDED_LOGICAL_FACADE_ONLY" ||
		r01StringField(t, contract, "first_user_space_component_contract") != "SUFFICIENT" ||
		r01StringField(t, contract, "internal_manager_dependency") != "NO" {
		t.Fatalf("pre_freeze_contract_design = %#v, want ACCEPTED/PASS/BOUNDED_IMPLEMENTED/CLOSED/PASS/ACCEPTED/PASS/BOUNDED_LOGICAL_FACADE_ONLY/SUFFICIENT/NO", contract)
	}
	reviewDisposition := r01MapField(t, contract, "review_disposition")
	for _, finding := range []string{"F-01", "F-02", "F-03", "F-04"} {
		if got := r01StringField(t, reviewDisposition, finding); got != "CLOSED" {
			t.Fatalf("pre_freeze_contract_design.review_disposition.%s = %q, want CLOSED", finding, got)
		}
	}
	f05 := r01MapField(t, reviewDisposition, "F-05")
	if r01StringField(t, f05, "finding") != "PAYLOAD_IDENTITY_SEMANTICS_NOT_REALIZABLE_AT_OPAQUE_KERNEL_BOUNDARY" || r01StringField(t, f05, "correction_status") != "CLOSED" || r01StringField(t, f05, "review_status") != "PASS" {
		t.Fatalf("pre_freeze_contract_design.review_disposition.F-05 = %#v, want finding with CLOSED/PASS", f05)
	}
	if r01StringField(t, reviewDisposition, "result") != "PASS" || r01StringField(t, reviewDisposition, "new_blocking_findings") != "NONE" || r01StringField(t, reviewDisposition, "payload_identity_model") != "ACCEPTED" || r01StringField(t, reviewDisposition, "runtime_scope_drift") != "NONE" {
		t.Fatalf("pre_freeze_contract_design.review_disposition = %#v, want accepted findings, no blockers, and no runtime drift", reviewDisposition)
	}
	implementationReviewDisposition := r01MapField(t, contract, "implementation_review_disposition")
	for _, finding := range []string{"IR-F01", "IR-F02", "IR-F03", "IR-F04"} {
		if got := r01StringField(t, implementationReviewDisposition, finding); got != "CLOSED" {
			t.Fatalf("pre_freeze_contract_design.implementation_review_disposition.%s = %q, want CLOSED", finding, got)
		}
	}
	if r01StringField(t, implementationReviewDisposition, "result") != "PASS" || r01StringField(t, implementationReviewDisposition, "new_design_findings") != "NONE" || r01StringField(t, implementationReviewDisposition, "new_runtime_findings") != "NONE" || r01StringField(t, implementationReviewDisposition, "runtime_scope_drift") != "NONE" {
		t.Fatalf("pre_freeze_contract_design.implementation_review_disposition = %#v, want PASS with no new findings or runtime drift", implementationReviewDisposition)
	}
	if r01StringField(t, contract, "bounded_implementation_authorized") != "YES" || r01StringField(t, contract, "milestone_closed") != "NO" {
		t.Fatalf("pre_freeze_contract_design authorization/closure = %#v, want YES/NO", contract)
	}
	assertCurrentDesignText(t, root, "execution-contract.md", []string{
		"InvocationBinding",
		"RequestID is never part of InvocationID identity",
		"INVOCATION_CONFLICT",
		"exact byte sequence equality",
		"False conflict is acceptable",
		"Kernel performs no schema-aware canonicalization",
		"ProviderCrossingAvailable=false",
		"NOT_KNOWN does not become safe to replay",
		"independently admitted trusted Resource-side authority source",
		"milestone_closed remains NO",
		"Physical ABI is DEFERRED",
		"Full Gate B is RESERVED",
		"Final Architecture Freeze is NOT_CLAIMED",
		"Mandatory EventRecord atomic coupling is limited",
		"Immediate definitive ENDED in DispatchExecution",
		"generic all-transitions event atomicity",
		"general multi-manager transaction",
		"global linearizability",
		"External Phase 2 resolution must remain visible",
		"facade-local marker is not occupancy authority",
		"allocation existence imply crossing",
	})
	assertCurrentDesignText(t, root, "resource-authority.md", []string{
		"sole authoritative source of capacity consumption",
		"CurrentOccupancy is the separate authoritative conclusion",
		"claim presence alone cannot distinguish NOT_ESTABLISHED from UNKNOWN",
		"independently admitted trusted Resource-side evidence source",
		"request cannot create, infer or elevate",
		"single controlled dispatch claim",
	})

}

func TestInvariant_PreFreezeGovernanceFixesAreRecorded(t *testing.T) {
	root := filepath.Join("..", "..")
	baseline := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "baseline.yaml"))
	readiness := r01MapField(t, baseline, "readiness")
	rawConstraints, ok := readiness["constraints"].([]any)
	if !ok {
		t.Fatalf("readiness.constraints = %#v, want a list of current constraints", readiness["constraints"])
	}
	constraints := make([]string, 0, len(rawConstraints))
	for index, rawConstraint := range rawConstraints {
		constraint, ok := rawConstraint.(string)
		if !ok {
			t.Fatalf("readiness.constraints[%d] = %#v, want string", index, rawConstraint)
		}
		constraints = append(constraints, constraint)
	}
	readinessText := strings.Join(constraints, "\n")
	for _, required := range []string{
		"Use the exact Resource claim set as the sole authoritative source for Resource execution-capacity claim accounting and available execution capacity.",
		"Treat ExecutionAllocation.CurrentOccupancy as the authoritative current per-allocation occupancy conclusion; keep it distinct from Resource claim accounting and invariant-consistent with the exact claim.",
	} {
		if !strings.Contains(readinessText, required) {
			t.Fatalf("current readiness constraints missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"Derive Resource occupancy and available capacity from the exact allocation-claim set as the single source of truth.",
		"claim set as the single source of current occupancy",
	} {
		if strings.Contains(strings.ToLower(readinessText), strings.ToLower(forbidden)) {
			t.Fatalf("stale ambiguous current readiness wording remains: %q", forbidden)
		}
	}

	assertCurrentDesignText(t, root, "execution-contract.md", []string{
		"Exact claim presence/absence",
		"neither capacity claims nor occupancy state substitutes for the other",
		"not business equivalence",
		"not occupancy authority",
	})

}

func TestInvariant_CurrentSourceOfTruthWordingIsQualified(t *testing.T) {
	root := filepath.Join("..", "..")
	invariants := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "invariants.yaml"))
	decisions := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "decisions.yaml"))

	resourceCapacity := r01EntryByID(t, invariants, "invariants", "id", "INV-RESOURCE-OWNS-EXECUTION-CAPACITY")
	if r01StringField(t, resourceCapacity, "status") != "FROZEN" || r01StringField(t, resourceCapacity, "name") != "Execution capacity is owned by Resource" {
		t.Fatalf("INV-RESOURCE-OWNS-EXECUTION-CAPACITY = %#v, want current FROZEN Resource-capacity invariant", resourceCapacity)
	}
	resourceCapacityRule := r01StringField(t, resourceCapacity, "rule")
	for _, required := range []string{
		"Resource owns execution capacity",
		"live exact allocation-claim set is the sole authoritative source for Resource execution-capacity claim accounting and available execution capacity",
		"ExecutionAllocation.CurrentOccupancy remains a separate authoritative per-allocation occupancy conclusion",
		"does not own Resource capacity",
		"cannot by itself distinguish NOT_ESTABLISHED from UNKNOWN",
	} {
		if !strings.Contains(resourceCapacityRule, required) {
			t.Fatalf("INV-RESOURCE-OWNS-EXECUTION-CAPACITY rule missing %q", required)
		}
	}

	claimAccounting := r01EntryByID(t, invariants, "invariants", "id", "INV-RESOURCE-CLAIM-SET-SOLE-OCCUPANCY-SOURCE")
	if r01StringField(t, claimAccounting, "status") != "FROZEN" || r01StringField(t, claimAccounting, "name") != "Resource capacity claim accounting has one source of truth" {
		t.Fatalf("INV-RESOURCE-CLAIM-SET-SOLE-OCCUPANCY-SOURCE = %#v, want narrowed current FROZEN invariant", claimAccounting)
	}
	claimAccountingRule := r01StringField(t, claimAccounting, "rule")
	for _, required := range []string{
		"Resource execution-capacity consumption and available execution capacity",
		"live exact allocation-claim set",
		"authoritative for Resource capacity accounting",
		"ExecutionAllocation.CurrentOccupancy remains authoritative for the current occupancy conclusion of one exact allocation",
		"distinct facts linked by invariants",
		"no parallel mutable capacity counter",
	} {
		if !strings.Contains(claimAccountingRule, required) {
			t.Fatalf("INV-RESOURCE-CLAIM-SET-SOLE-OCCUPANCY-SOURCE rule missing %q", required)
		}
	}

	resourceClaimDecision := r01EntryByID(t, decisions, "decisions", "id", "D-GATE-B-PHASE-1-RESOURCE-CLAIM-SINGLE-SOURCE")
	if r01StringField(t, resourceClaimDecision, "status") != "FROZEN" || r01StringField(t, resourceClaimDecision, "title") != "Resource claims are the sole Resource execution-capacity claim-accounting source" {
		t.Fatalf("D-GATE-B-PHASE-1-RESOURCE-CLAIM-SINGLE-SOURCE = %#v, want narrowed current FROZEN decision", resourceClaimDecision)
	}
	decisionText := strings.Join([]string{
		r01StringField(t, resourceClaimDecision, "title"),
		r01StringField(t, resourceClaimDecision, "decision"),
		r01StringField(t, resourceClaimDecision, "rationale"),
	}, "\n")
	for _, required := range []string{
		"Resource claims are authoritative for Resource execution-capacity ownership and capacity accounting",
		"live exact allocation-claim set determines which exact execution allocations consume Resource capacity",
		"current consumed capacity",
		"available execution capacity",
		"ExecutionAllocation.CurrentOccupancy remains the authoritative per-allocation current occupancy conclusion",
		"is not owned by the Resource claim set",
		"without claiming all per-allocation occupancy information",
	} {
		if !strings.Contains(decisionText, required) {
			t.Fatalf("D-GATE-B-PHASE-1-RESOURCE-CLAIM-SINGLE-SOURCE text missing %q", required)
		}
	}

	for record, text := range map[string]string{
		"INV-RESOURCE-OWNS-EXECUTION-CAPACITY":          resourceCapacityRule,
		"INV-RESOURCE-CLAIM-SET-SOLE-OCCUPANCY-SOURCE":  claimAccountingRule,
		"D-GATE-B-PHASE-1-RESOURCE-CLAIM-SINGLE-SOURCE": decisionText,
	} {
		lower := strings.ToLower(text)
		for _, forbidden := range []string{
			"sole occupancy source",
			"complete authoritative occupancy information",
			"all authoritative per-allocation occupancy information",
			"sole source of current occupancy",
		} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s retains unqualified current wording %q", record, forbidden)
			}
		}
	}
}

func TestInvariant_ArchitectureStatusAssignmentsRemainExplicit(t *testing.T) {
	baseline := parseR01ArchitectureYAML(t, filepath.Join("..", "..", "architecture", "baseline.yaml"))
	state := r01MapField(t, baseline, "state")
	if r01StringField(t, state, "working_tree_status") != "FROZEN" {
		t.Fatalf("state.working_tree_status = %q, want FROZEN", r01StringField(t, state, "working_tree_status"))
	}
	currentPhase := r01MapField(t, baseline, "current_phase")
	if r01StringField(t, currentPhase, "status") != "CLOSED" || r01StringField(t, currentPhase, "design_status") != "ACCEPTED" || r01StringField(t, currentPhase, "review_status") != "PASS" || r01StringField(t, currentPhase, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, currentPhase, "implementation_readiness") != "CLOSED" || r01StringField(t, currentPhase, "implementation_review_status") != "PASS" {
		t.Fatalf("current_phase = %#v, want CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS", currentPhase)
	}
	if r01StringField(t, currentPhase, "id") != "GATE-B-PHASE-2" {
		t.Fatalf("current_phase.id = %q, want GATE-B-PHASE-2", r01StringField(t, currentPhase, "id"))
	}
	currentSubphase := r01MapField(t, currentPhase, "current_subphase")
	if r01StringField(t, currentSubphase, "id") != "EXECUTION-RESOLUTION-MECHANISM" || r01StringField(t, currentSubphase, "status") != "CLOSED" || r01StringField(t, currentSubphase, "design_status") != "ACCEPTED" || r01StringField(t, currentSubphase, "review_status") != "PASS" || r01StringField(t, currentSubphase, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, currentSubphase, "implementation_readiness") != "CLOSED" || r01StringField(t, currentSubphase, "implementation_review_status") != "PASS" {
		t.Fatalf("current_phase.current_subphase = %#v, want EXECUTION-RESOLUTION-MECHANISM/CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS", currentSubphase)
	}
	milestone := r01MapField(t, baseline, "milestone")
	if r01StringField(t, milestone, "id") != "GATE-B-PHASE-2" || r01StringField(t, milestone, "status") != "CLOSED" || r01StringField(t, milestone, "design_status") != "ACCEPTED" || r01StringField(t, milestone, "review_status") != "PASS" || r01StringField(t, milestone, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, milestone, "implementation_readiness") != "CLOSED" || r01StringField(t, milestone, "implementation_review_status") != "PASS" || r01StringField(t, milestone, "milestone_closed") != "CLOSED" {
		t.Fatalf("milestone = %#v, want GATE-B-PHASE-2/CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS/CLOSED", milestone)
	}
	intentBoundary := r01MapField(t, baseline, "intent_boundary")
	for _, want := range []struct {
		field string
		value string
	}{
		{field: "status", value: "FROZEN_SEMANTICS_ONLY"},
		{field: "implementation_status", value: "NOT_IMPLEMENTED"},
		{field: "final_architecture_freeze", value: "NOT_CLAIMED"},
	} {
		if got := r01StringField(t, intentBoundary, want.field); got != want.value {
			t.Fatalf("intent_boundary.%s = %q, want %q", want.field, got, want.value)
		}
	}

	freeze := r01MapField(t, baseline, "freeze")
	semanticContracts := r01MapField(t, freeze, "semantic_contracts")
	if got := strings.TrimSpace(r01StringField(t, semanticContracts, "K2_C")); got == "" {
		t.Fatal("freeze.semantic_contracts.K2_C must retain an explicit progress marker")
	}
	if r01StringField(t, semanticContracts, "K2_C") != "BOUNDED_IMPLEMENTED" || r01StringField(t, semanticContracts, "K2_D") != "CLOSED" || r01StringField(t, semanticContracts, "K2_E") != "CLOSED" || r01StringField(t, semanticContracts, "GATE_B_PHASE_1") != "CLOSED" || r01StringField(t, semanticContracts, "GATE_B_PHASE_2") != "CLOSED" {
		t.Fatalf("freeze.semantic_contracts = %#v, want K2-C/K2-D/K2-E and Gate B Phase 1/Phase 2 closed markers", semanticContracts)
	}
	governanceGates := r01MapField(t, freeze, "governance_gates")
	for _, want := range []struct {
		field string
		value string
	}{
		{field: "K1_review_document_human_signoff", value: "OPEN"},
		{field: "R0_final_architecture_freeze", value: "NOT_CLAIMED"},
	} {
		if got := r01StringField(t, governanceGates, want.field); got != want.value {
			t.Fatalf("freeze.governance_gates.%s = %q, want %q", want.field, got, want.value)
		}
	}
	convergence := r01MapField(t, baseline, "convergence")
	if r01StringField(t, convergence, "status") != "COMPLETE" || r01StringField(t, convergence, "implementation_status") != "BOUNDED_LOCAL_EVIDENCE" {
		t.Fatalf("convergence = %#v, want COMPLETE/BOUNDED_LOCAL_EVIDENCE", convergence)
	}
	if r01StringField(t, convergence, "local_admission_bridge") != "IMPLEMENTED_FOR_K2_E" || r01StringField(t, convergence, "broader_resource_admission") != "OPEN" {
		t.Fatalf("convergence admission states = %#v, want local IMPLEMENTED_FOR_K2_E and broader OPEN", convergence)
	}
	readiness := r01MapField(t, baseline, "readiness")
	if r01StringField(t, readiness, "decision") != "GATE_B_PHASE_2_CLOSED" || r01StringField(t, readiness, "status") != "CLOSED" || r01StringField(t, readiness, "design_status") != "ACCEPTED" || r01StringField(t, readiness, "review_status") != "PASS" || r01StringField(t, readiness, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, readiness, "implementation_readiness") != "CLOSED" || r01StringField(t, readiness, "implementation_review_status") != "PASS" {
		t.Fatalf("readiness = %#v, want Gate B Phase 2 CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS", readiness)
	}
	if r01StringField(t, readiness, "final_architecture_freeze") != "NOT_CLAIMED" {
		t.Fatalf("readiness.final_architecture_freeze = %q, want NOT_CLAIMED", r01StringField(t, readiness, "final_architecture_freeze"))
	}
	successCriteria := r01MapField(t, baseline, "success_criteria")
	gateA := r01EntryByID(t, successCriteria, "gates", "id", "GATE-A-RUNTIME-FOUNDATION")
	gatePhase2 := r01EntryByID(t, successCriteria, "gates", "id", "GATE-B-PHASE-2-EXECUTION-RESOLUTION")
	gateB := r01EntryByID(t, successCriteria, "gates", "id", "GATE-B-AUTONOMOUS-KERNEL-LOOP")
	if r01StringField(t, gateA, "implementation_status") != "BOUNDED_IMPLEMENTED" {
		t.Fatalf("Gate A implementation_status = %q, want BOUNDED_IMPLEMENTED", r01StringField(t, gateA, "implementation_status"))
	}
	if r01StringField(t, gateB, "status") != "RESERVED" || r01StringField(t, gateB, "implementation_status") != "PHASE_2_BOUNDED_IMPLEMENTED" {
		t.Fatalf("Gate B = %#v, want RESERVED/PHASE_2_BOUNDED_IMPLEMENTED", gateB)
	}
	if r01StringField(t, gatePhase2, "status") != "CLOSED" || r01StringField(t, gatePhase2, "design_status") != "ACCEPTED" || r01StringField(t, gatePhase2, "review_status") != "PASS" || r01StringField(t, gatePhase2, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, gatePhase2, "implementation_readiness") != "CLOSED" || r01StringField(t, gatePhase2, "implementation_review_status") != "PASS" {
		t.Fatalf("Gate B Phase 2 = %#v, want CLOSED/design ACCEPTED/BOUNDED_IMPLEMENTED/CLOSED/PASS", gatePhase2)
	}
	for _, want := range []struct {
		id             string
		status         string
		implementation string
	}{
		{id: "K2-C", status: "ACCEPTED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "K2-D", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "K2-E", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "GATE-B-PHASE-1", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "GATE-B-PHASE-2", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
	} {
		entry := r01EntryByID(t, baseline, "milestone_history", "id", want.id)
		if got := r01StringField(t, entry, "status"); got != want.status {
			t.Fatalf("milestone_history[%s].status = %q, want %q", want.id, got, want.status)
		}
		if got := r01StringField(t, entry, "implementation_status"); got != want.implementation {
			t.Fatalf("milestone_history[%s].implementation_status = %q, want %q", want.id, got, want.implementation)
		}
	}

	for _, want := range []struct {
		id     string
		status string
	}{
		{id: "ExecutionAuthorityScope", status: "FROZEN"},
		{id: "AutonomyScope", status: "RESERVED"},
		{id: "IntentScope", status: "RESERVED"},
		{id: "RoleScope", status: "RESERVED"},
		{id: "MeshOrRegionScope", status: "DEFERRED"},
	} {
		entry := r01EntryByID(t, baseline, "scope_domains", "id", want.id)
		if got := r01StringField(t, entry, "status"); got != want.status {
			t.Fatalf("scope_domains[%s].status = %q, want %q", want.id, got, want.status)
		}
	}

	decisions := parseR01ArchitectureYAML(t, filepath.Join("..", "..", "architecture", "decisions.yaml"))
	for _, want := range []struct {
		id     string
		status string
	}{
		{id: "D-TASK-EXTERNAL-POLICY-MANAGEMENT", status: "FROZEN_SEMANTICS_ONLY"},
		{id: "D-GATE-B-PHASE-2-EXECUTION-RESOLUTION", status: "ACCEPTED"},
		{id: "D-R0-DEFERRAL-NOT-NEGATION", status: "FROZEN"},
		{id: "D-R0-CHECKPOINT-NOT-FINAL-FREEZE", status: "FROZEN"},
		{id: "K1-OPEN-CROSS-MANAGER-CONSISTENCY", status: "OPEN"},
		{id: "K1-DEFERRED-SYSCALL-ABI", status: "DEFERRED"},
		{id: "K1-OPEN-AUTHORIZATION", status: "OPEN"},
		{id: "K1-DEFERRED-SCOPE-GOVERNANCE", status: "DEFERRED"},
	} {
		entry := r01EntryByID(t, decisions, "decisions", "id", want.id)
		if got := r01StringField(t, entry, "status"); got != want.status {
			t.Fatalf("decisions[%s].status = %q, want %q", want.id, got, want.status)
		}
	}
}

func TestInvariant_AllArchitectureStatusAssignmentsRemainExplicit(t *testing.T) {
	root := filepath.Join("..", "..")
	expected := architectureStatusExpectations(t)
	actual := make(map[string]string)
	architectureDir := filepath.Join(root, "architecture")
	entries, err := os.ReadDir(architectureDir)
	if err != nil {
		t.Fatalf("read architecture directory: %v", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatal("architecture directory contains no YAML state files")
	}
	for _, file := range files {
		document := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", file))
		collectArchitectureStatusFields(file, document, "", actual)
	}

	for _, key := range sortedArchitectureStatusKeys(expected) {
		want := expected[key]
		got, ok := actual[key]
		if !ok {
			t.Errorf("architecture status manifest entry %q is missing", key)
			continue
		}
		if want == architectureStatusAnyExplicit {
			if strings.TrimSpace(got) == "" {
				t.Errorf("architecture status %q is empty, want an explicit status value", key)
			}
			continue
		}
		if got != want {
			t.Errorf("architecture status %q = %q, want %q", key, got, want)
		}
	}
	for _, key := range sortedArchitectureStatusKeys(actual) {
		if _, ok := expected[key]; !ok {
			t.Errorf("architecture status field %q is not covered by the explicit manifest", key)
		}
	}
	if len(actual) != len(expected) {
		t.Fatalf("architecture status manifest covers %d fields, parsed %d fields", len(expected), len(actual))
	}
}

func architectureStatusExpectations(t *testing.T) map[string]string {
	t.Helper()
	expected := make(map[string]string)
	add := func(file, path, field, value string) {
		key := architectureStatusKey(file, path, field)
		if _, exists := expected[key]; exists {
			t.Fatalf("duplicate architecture status manifest entry %q", key)
		}
		expected[key] = value
	}
	addEntries := func(file, collection, identityField, status string, entries ...string) {
		for _, entry := range entries {
			add(file, collection+"#"+identityField+"="+entry, "status", status)
		}
	}

	add("baseline.yaml", "milestone#id=GATE-B-PHASE-2", "status", "CLOSED")
	add("baseline.yaml", "milestone#id=GATE-B-PHASE-2", "design_status", "ACCEPTED")
	add("baseline.yaml", "milestone#id=GATE-B-PHASE-2", "review_status", "PASS")
	add("baseline.yaml", "milestone#id=GATE-B-PHASE-2", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "milestone#id=GATE-B-PHASE-2", "implementation_readiness", "CLOSED")
	add("baseline.yaml", "milestone#id=GATE-B-PHASE-2", "implementation_review_status", "PASS")
	add("baseline.yaml", "state#id=DTM-ARCHITECTURE-STATE", "working_tree_status", "FROZEN")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2", "status", "CLOSED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2", "design_status", "ACCEPTED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2", "review_status", "PASS")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2", "implementation_readiness", "CLOSED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2", "implementation_review_status", "PASS")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2.current_subphase#id=EXECUTION-RESOLUTION-MECHANISM", "status", "CLOSED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2.current_subphase#id=EXECUTION-RESOLUTION-MECHANISM", "design_status", "ACCEPTED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2.current_subphase#id=EXECUTION-RESOLUTION-MECHANISM", "review_status", "PASS")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2.current_subphase#id=EXECUTION-RESOLUTION-MECHANISM", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2.current_subphase#id=EXECUTION-RESOLUTION-MECHANISM", "implementation_readiness", "CLOSED")
	add("baseline.yaml", "current_phase#id=GATE-B-PHASE-2.current_subphase#id=EXECUTION-RESOLUTION-MECHANISM", "implementation_review_status", "PASS")
	add("baseline.yaml", "checkpoint#id=R0-ARCHITECTURE-REENTRY", "status", "FREEZE_CANDIDATE")
	add("baseline.yaml", "checkpoint#id=R0-ARCHITECTURE-REENTRY", "final_architecture_freeze", "NOT_CLAIMED")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN", "status", "ACCEPTED")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN", "design_status", "ACCEPTED")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN", "review_status", "PASS")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN", "implementation_readiness", "CLOSED")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN", "implementation_review_status", "PASS")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN", "implementation_fix_status", "ACCEPTED")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN.review_disposition.F-05", "correction_status", "CLOSED")
	add("baseline.yaml", "pre_freeze_contract_design#id=DTM-KERNEL-PRE-FREEZE-CONTRACT-DESIGN.review_disposition.F-05", "review_status", "PASS")
	add("baseline.yaml", "intent_boundary#id=R0.1-INTENT-BOUNDARY", "status", "FROZEN_SEMANTICS_ONLY")
	add("baseline.yaml", "intent_boundary#id=R0.1-INTENT-BOUNDARY", "implementation_status", "NOT_IMPLEMENTED")
	add("baseline.yaml", "intent_boundary#id=R0.1-INTENT-BOUNDARY", "current_object_reference_status", "ObjectKindIntent remains rejected by the current K2 Kernel ObjectReference validation; no Intent ObjectReference is enabled.")
	add("baseline.yaml", "intent_boundary#id=R0.1-INTENT-BOUNDARY", "final_architecture_freeze", "NOT_CLAIMED")
	add("baseline.yaml", "readiness", "status", "CLOSED")
	add("baseline.yaml", "readiness", "design_status", "ACCEPTED")
	add("baseline.yaml", "readiness", "review_status", "PASS")
	add("baseline.yaml", "readiness", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "readiness", "implementation_readiness", "CLOSED")
	add("baseline.yaml", "readiness", "implementation_review_status", "PASS")
	add("baseline.yaml", "readiness", "final_architecture_freeze", "NOT_CLAIMED")
	for _, milestone := range []struct {
		id             string
		status         string
		implementation string
	}{
		{id: "K2-C", status: "ACCEPTED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "K2-D", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "K2-E", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "GATE-B-PHASE-1", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
		{id: "GATE-B-PHASE-2", status: "CLOSED", implementation: "BOUNDED_IMPLEMENTED"},
	} {
		add("baseline.yaml", "milestone_history#id="+milestone.id, "status", milestone.status)
		add("baseline.yaml", "milestone_history#id="+milestone.id, "implementation_status", milestone.implementation)
	}
	add("baseline.yaml", "convergence#id=WORLD-A-WORLD-B-LOCAL-CONVERGENCE", "status", "COMPLETE")
	add("baseline.yaml", "convergence#id=WORLD-A-WORLD-B-LOCAL-CONVERGENCE", "implementation_status", "BOUNDED_LOCAL_EVIDENCE")
	add("baseline.yaml", "source_authority.excluded_paths#path=exports/legacy-artifacts/pre-refactor-hygiene-2026-09-06/architercture-2026-09-06/", "status", "HISTORICAL_ARTIFACT")
	for source, status := range map[string]string{
		"K0.5-R1 Freeze Candidate":                               "HISTORICAL_CANDIDATE",
		"K1 Architecture Freeze Review":                          "SEMANTIC_FREEZE_WITH_OPEN_SIGNOFF",
		"K2-A and K2-B design documents":                         "DESIGN_ONLY",
		"R0 Architecture Re-entry & Repair":                      "CARRIED_FORWARD_CHECKPOINT",
		"R0.1 UserIntent / KernelIntent Boundary Freeze":         "CURRENT_SEMANTIC_FREEZE",
		"R0.1 KernelIntent Persistent Representation Correction": "CURRENT_SEMANTIC_CORRECTION",
		"K2-C Handle Validation Framework":                       "ACCEPTED",
		"K2-D Invariant Test Suite":                              "CLOSED",
		"K2-E Local Runtime Convergence Demo":                    "CLOSED",
		"Gate B Phase 1 Resource Execution Ownership":            "CLOSED",
		"Gate B Phase 1 Closure Record":                          "CLOSED",
		"Gate B Phase 2 Execution Resolution Mechanism":          "CLOSED",
	} {
		add("baseline.yaml", "freeze.supersession_chain#source="+source, "status", status)
	}
	add("baseline.yaml", "freeze.supersession_chain#source=Gate B Phase 2 Execution Resolution Mechanism", "design_status", "ACCEPTED")
	add("baseline.yaml", "freeze.supersession_chain#source=Gate B Phase 2 Execution Resolution Mechanism", "review_status", "PASS")
	add("baseline.yaml", "freeze.supersession_chain#source=Gate B Phase 2 Execution Resolution Mechanism", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "freeze.supersession_chain#source=Gate B Phase 2 Execution Resolution Mechanism", "implementation_readiness", "CLOSED")
	add("baseline.yaml", "freeze.supersession_chain#source=Gate B Phase 2 Execution Resolution Mechanism", "implementation_review_status", "PASS")
	addEntries("baseline.yaml", "scope_domains", "id", "FROZEN", "ExecutionAuthorityScope")
	addEntries("baseline.yaml", "scope_domains", "id", "RESERVED", "AutonomyScope", "IntentScope", "RoleScope")
	addEntries("baseline.yaml", "scope_domains", "id", "DEFERRED", "MeshOrRegionScope")
	add("baseline.yaml", "success_criteria.gates#id=GATE-A-RUNTIME-FOUNDATION", "status", "FROZEN")
	add("baseline.yaml", "success_criteria.gates#id=GATE-A-RUNTIME-FOUNDATION", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-PHASE-2-EXECUTION-RESOLUTION", "status", "CLOSED")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-PHASE-2-EXECUTION-RESOLUTION", "design_status", "ACCEPTED")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-PHASE-2-EXECUTION-RESOLUTION", "review_status", "PASS")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-PHASE-2-EXECUTION-RESOLUTION", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-PHASE-2-EXECUTION-RESOLUTION", "implementation_readiness", "CLOSED")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-PHASE-2-EXECUTION-RESOLUTION", "implementation_review_status", "PASS")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-AUTONOMOUS-KERNEL-LOOP", "status", "RESERVED")
	add("baseline.yaml", "success_criteria.gates#id=GATE-B-AUTONOMOUS-KERNEL-LOOP", "implementation_status", "PHASE_2_BOUNDED_IMPLEMENTED")
	add("baseline.yaml", "freeze.governance_gates", "R0_final_architecture_freeze", "NOT_CLAIMED")
	for id, status := range map[string]string{
		"Discovery":               "DEFERRED",
		"Membership":              "RESERVED",
		"RoleSelection":           "RESERVED",
		"RoleMigration":           "RESERVED",
		"MultiCore":               "RESERVED",
		"Gateway":                 "RESERVED",
		"AutonomyScopeHierarchy":  "DEFERRED",
		"RemoteTrust":             "DEFERRED",
		"CrossManagerConsistency": "OPEN",
		"CrossFenceResolution":    "RESERVED",
		"IntentReconciliation":    "DEFERRED",
	} {
		add("baseline.yaml", "reserved_future_mechanisms#id="+id, "status", status)
	}

	addEntries("boundaries.yaml", "boundaries", "layer", "FROZEN", "Kernel", "Observation/Event", "Scope Domains", "Application/UserSpace")
	addEntries("boundaries.yaml", "boundaries", "layer", "FROZEN_SEMANTICS_ONLY", "UserIntent / KernelIntent Admission")
	addEntries("boundaries.yaml", "boundaries", "layer", "OPEN", "Resource Registration / Admission")
	addEntries("boundaries.yaml", "boundaries", "layer", "ACCEPTED", "Execution Resolution")
	add("boundaries.yaml", "boundaries#layer=Execution Resolution", "design_status", "ACCEPTED")
	add("boundaries.yaml", "boundaries#layer=Execution Resolution", "review_status", "PASS")
	add("boundaries.yaml", "boundaries#layer=Execution Resolution", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("boundaries.yaml", "boundaries#layer=Execution Resolution", "implementation_readiness", "CLOSED")
	add("boundaries.yaml", "boundaries#layer=Execution Resolution", "implementation_review_status", "PASS")
	addEntries("boundaries.yaml", "boundaries", "layer", "RESERVED", "Autonomous Kernel Loop")
	addEntries("boundaries.yaml", "boundaries", "layer", "NEEDS_REVIEW", "Runtime", "SDK")

	for id, status := range map[string]string{
		"Resource":                     "FROZEN",
		"Capability":                   "NEEDS_REVIEW",
		"CapabilityDeclaration":        "FROZEN",
		"CapabilityInstance":           "FROZEN",
		"CapabilityHandle":             "FROZEN",
		"Task":                         "FROZEN_SEMANTICS_ONLY",
		"Intent":                       "FROZEN",
		"UserIntent":                   "FROZEN_SEMANTICS_ONLY",
		"KernelIntentSpec":             "FROZEN_SEMANTICS_ONLY",
		"KernelIntent":                 "FROZEN_SEMANTICS_ONLY",
		"Observation":                  "NEEDS_REVIEW",
		"KernelStateTransition":        "FROZEN",
		"ProviderDeclaration":          "DEFERRED",
		"RegistrationRequest":          "DEFERRED",
		"EventRecord":                  "FROZEN",
		"ExecutionContext":             "FROZEN",
		"ExecutionRequest":             "BOUNDED_IMPLEMENTED",
		"ExecutionDescriptor":          "BOUNDED_IMPLEMENTED",
		"ExecutionAllocation":          "BOUNDED_IMPLEMENTED",
		"ExecutionObservation":         "BOUNDED_IMPLEMENTED",
		"CurrentOccupancy":             "ACCEPTED",
		"ResolutionEvidence":           "ACCEPTED",
		"OccupancyResolutionAuthority": "ACCEPTED",
		"OwnershipFence":               "ACCEPTED",
		"State":                        "NEEDS_REVIEW",
		"Memory":                       "NEEDS_REVIEW",
		"Context":                      "NEEDS_REVIEW",
		"Membership":                   "NEEDS_REVIEW",
		"ExecutionAuthorityScope":      "FROZEN",
		"AutonomyScope":                "RESERVED",
		"IntentScope":                  "RESERVED",
		"RoleScope":                    "RESERVED",
		"AutonomousLoop":               "RESERVED",
		"Runtime":                      "NEEDS_REVIEW",
	} {
		add("concepts.yaml", "concepts#ConceptID="+id, "status", status)
	}
	for _, id := range []string{"CurrentOccupancy", "ResolutionEvidence", "OccupancyResolutionAuthority", "OwnershipFence"} {
		add("concepts.yaml", "concepts#ConceptID="+id, "design_status", "ACCEPTED")
		add("concepts.yaml", "concepts#ConceptID="+id, "review_status", "PASS")
		add("concepts.yaml", "concepts#ConceptID="+id, "implementation_status", "BOUNDED_IMPLEMENTED")
		add("concepts.yaml", "concepts#ConceptID="+id, "implementation_readiness", "CLOSED")
		add("concepts.yaml", "concepts#ConceptID="+id, "implementation_review_status", "PASS")
	}

	for _, id := range []string{
		"D-KERNEL-VERSION-AXES",
		"D-KERNEL-OBJECT-FOUNDATION",
		"D-RESOURCE-CAPABILITY-SEPARATION",
		"D-CAPABILITY-HANDLE-AUTHORITY",
		"D-EXECUTION-CONTEXT-SCOPE",
		"D-EVENT-IMMUTABLE-FACT",
		"D-LIFECYCLE-AVAILABILITY-SEPARATION",
		"D-PERMISSIONSET-METADATA",
		"D-MANAGER-API-INTERNAL",
		"D-INTENT-USERSPACE",
		"D-R0-INTENT-REFERENCE-BOUNDARY",
		"D-R0-OBSERVATION-EVENT-SEPARATION",
		"D-R0-RESOURCE-ADMISSION-PRESERVATION",
		"D-R0-SCOPE-DOMAIN-SEPARATION",
		"D-R0-LOCAL-AVAILABILITY-PROVENANCE",
		"D-R0-TWO-GATE-SUCCESS-CRITERIA",
		"D-GATE-B-PHASE-1-EXECUTION-OWNERSHIP",
		"D-GATE-B-PHASE-1-PROVIDER-DISPATCH-RESOLUTION",
		"D-GATE-B-PHASE-1-RESOURCE-CLAIM-SINGLE-SOURCE",
		"D-R0-SOURCE-AUTHORITY",
		"D-R0-DEFERRAL-NOT-NEGATION",
		"D-R0-K1-SEMANTIC-FREEZE",
		"D-R0-CHECKPOINT-NOT-FINAL-FREEZE",
		"D-R1-SCOPE",
		"D-LEGACY-COMPATIBILITY",
	} {
		add("decisions.yaml", "decisions#id="+id, "status", "FROZEN")
	}
	add("decisions.yaml", "decisions#id=D-R0.1-INTENT-DUAL-LAYER", "status", "FROZEN_SEMANTICS_ONLY")
	add("decisions.yaml", "decisions#id=D-R0.1-INTENT-DUAL-LAYER", "implementation_status", "Architecture semantics only; no Intent runtime implementation is admitted by this decision.")
	add("decisions.yaml", "decisions#id=D-R0.1-INTENT-PERSISTENT-REPRESENTATION", "status", "FROZEN_SEMANTICS_ONLY")
	add("decisions.yaml", "decisions#id=D-R0.1-INTENT-PERSISTENT-REPRESENTATION", "implementation_status", "Architecture semantics only; no Intent runtime, evaluator, World Model, or reconciliation engine is admitted by this decision.")
	add("decisions.yaml", "decisions#id=D-R0-OBSERVATION-EVENT-SEPARATION", "implementation_status", "K2-E consumes an already authoritative local ResourceRecordView for bounded convergence; external ingress, freshness, contradiction, and remote evidence mechanisms remain OPEN/DEFERRED.")
	add("decisions.yaml", "decisions#id=D-TASK-EXTERNAL-POLICY-MANAGEMENT", "status", "FROZEN_SEMANTICS_ONLY")
	add("decisions.yaml", "decisions#id=D-GATE-B-PHASE-2-EXECUTION-RESOLUTION", "status", "ACCEPTED")
	add("decisions.yaml", "decisions#id=D-GATE-B-PHASE-2-EXECUTION-RESOLUTION", "design_status", "ACCEPTED")
	add("decisions.yaml", "decisions#id=D-GATE-B-PHASE-2-EXECUTION-RESOLUTION", "review_status", "PASS")
	add("decisions.yaml", "decisions#id=D-GATE-B-PHASE-2-EXECUTION-RESOLUTION", "implementation_status", "BOUNDED_IMPLEMENTED")
	add("decisions.yaml", "decisions#id=D-GATE-B-PHASE-2-EXECUTION-RESOLUTION", "implementation_readiness", "CLOSED")
	add("decisions.yaml", "decisions#id=D-GATE-B-PHASE-2-EXECUTION-RESOLUTION", "implementation_review_status", "PASS")
	for id, status := range map[string]string{
		"K2-OPEN-RESOURCE-ADMISSION-BRIDGE": "OPEN",
		"K1-OPEN-CROSS-MANAGER-CONSISTENCY": "OPEN",
		"K1-DEFERRED-SYSCALL-ABI":           "DEFERRED",
		"K1-OPEN-AUTHORIZATION":             "OPEN",
		"K1-DEFERRED-SCOPE-GOVERNANCE":      "DEFERRED",
		"K1-DEFERRED-EVENT-PERSISTENCE":     "DEFERRED",
		"K1-DEFERRED-REMOTE-INVOCATION":     "DEFERRED",
		"K1-OPEN-REVIEW-SIGNOFF":            "OPEN",
	} {
		add("decisions.yaml", "decisions#id="+id, "status", status)
	}

	for _, id := range []string{
		"INV-KERNEL-USERSPACE-BOUNDARY",
		"INV-INTENT-USERSPACE",
		"INV-RESOURCE-CAPABILITY-SEPARATION",
		"INV-CAPABILITY-AUTHORITY",
		"INV-HANDLE-CONTEXT-SCOPE",
		"INV-LIFECYCLE-AVAILABILITY-SEPARATION",
		"INV-EVENT-IMMUTABLE-SEMANTIC",
		"INV-QUERY-PURE-SNAPSHOT",
		"INV-AUDIT-ROLLBACK",
		"INV-FAIL-CLOSED-AUTHORITY",
		"INV-UNKNOWN-NO-BLIND-RETRY",
		"INV-RESOURCE-LIFECYCLE-SUBSET",
		"INV-OBSERVATION-EVIDENCE-SEPARATION",
		"INV-RESOURCE-ADMISSION-BEFORE-AUTHORITY",
		"INV-SCOPE-DOMAIN-SEPARATION",
		"INV-LOCAL-AVAILABILITY-PROVENANCE",
		"INV-GATE-A-IS-NOT-GATE-B",
		"INV-ALLOCATION-IS-NOT-AUTHORITY",
		"INV-RESOURCE-OWNS-EXECUTION-CAPACITY",
		"INV-ALLOCATION-DISPATCH-BARRIER",
		"INV-ALLOCATION-PROVIDER-DISPATCH-RESOLUTION",
		"INV-RESOURCE-CLAIM-SET-SOLE-OCCUPANCY-SOURCE",
		"INV-RESOURCE-CLAIM-PRIMITIVES-ARE-INTERNAL",
		"INV-OBSERVATION-OUTCOME-OCCUPANCY-SEPARATION",
		"INV-UNKNOWN-OCCUPANCY-NO-RELEASE",
		"INV-DEFERRED-NOT-NEGATED",
		"INV-R0-CHECKPOINT-NOT-FINAL-FREEZE",
	} {
		add("invariants.yaml", "invariants#id="+id, "status", "FROZEN")
	}
	for _, id := range []string{
		"INV-TASK-EXTERNAL-POLICY-MANAGED",
		"INV-R0.1-INTENT-LAYER-SEPARATION",
		"INV-R0.1-KERNELINTENT-NOT-EXECUTION-AUTHORITY",
		"INV-R0.1-EVALUATION-REQUIRES-OBSERVATION",
	} {
		add("invariants.yaml", "invariants#id="+id, "status", "FROZEN_SEMANTICS_ONLY")
	}
	add("invariants.yaml", "invariants#id=INV-R0.1-KERNELINTENT-PERSISTENT-OPAQUE", "status", "FROZEN_SEMANTICS_ONLY")
	for _, id := range []string{
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
		add("invariants.yaml", "invariants#id="+id, "status", "ACCEPTED")
		add("invariants.yaml", "invariants#id="+id, "design_status", "ACCEPTED")
		add("invariants.yaml", "invariants#id="+id, "review_status", "PASS")
		add("invariants.yaml", "invariants#id="+id, "implementation_status", "BOUNDED_IMPLEMENTED")
		add("invariants.yaml", "invariants#id="+id, "implementation_readiness", "CLOSED")
		add("invariants.yaml", "invariants#id="+id, "implementation_review_status", "PASS")
	}

	return expected
}

func collectArchitectureStatusFields(file string, value any, path string, result map[string]string) {
	switch typed := value.(type) {
	case map[string]any:
		currentPath := path
		if identity, ok := architectureEntryIdentity(typed); ok {
			currentPath += "#" + identity
		}
		for key, child := range typed {
			if isArchitectureStatusField(key) {
				status, ok := child.(string)
				if !ok {
					panic(fmt.Sprintf("%s:%s is not a string", file, currentPath+":"+key))
				}
				result[architectureStatusKey(file, currentPath, key)] = status
				continue
			}
			childPath := key
			if currentPath != "" {
				childPath = currentPath + "." + key
			}
			collectArchitectureStatusFields(file, child, childPath, result)
		}
	case []any:
		for _, child := range typed {
			collectArchitectureStatusFields(file, child, path, result)
		}
	}
}

func isArchitectureStatusField(key string) bool {
	return key == "status" ||
		key == "implementation_status" ||
		strings.HasSuffix(key, "_status") ||
		strings.HasSuffix(key, "_readiness") ||
		strings.HasSuffix(key, "_freeze")
}

const architectureStatusAnyExplicit = "<ANY_EXPLICIT_STATUS>"

func architectureEntryIdentity(document map[string]any) (string, bool) {
	for _, field := range []string{"id", "ConceptID", "layer", "path", "source"} {
		value, ok := document[field].(string)
		if ok {
			return field + "=" + value, true
		}
	}
	return "", false
}

func architectureStatusKey(file, path, field string) string {
	return file + "|" + path + ":" + field
}

func sortedArchitectureStatusKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestInvariant_R0CheckpointIsNotFinalArchitectureFreeze(t *testing.T) {
	baseline := parseR01ArchitectureYAML(t, filepath.Join("..", "..", "architecture", "baseline.yaml"))
	checkpoint := r01MapField(t, baseline, "checkpoint")
	if r01StringField(t, checkpoint, "status") != "FREEZE_CANDIDATE" {
		t.Fatalf("checkpoint.status = %q, want FREEZE_CANDIDATE", r01StringField(t, checkpoint, "status"))
	}
	if r01StringField(t, checkpoint, "final_architecture_freeze") != "NOT_CLAIMED" {
		t.Fatalf("checkpoint.final_architecture_freeze = %q, want NOT_CLAIMED", r01StringField(t, checkpoint, "final_architecture_freeze"))
	}
}
