package kernel_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestR01ArchitectureStateContract(t *testing.T) {
	root := filepath.Join("..", "..")
	paths := []string{
		filepath.Join(root, "architecture", "baseline.yaml"),
		filepath.Join(root, "architecture", "boundaries.yaml"),
		filepath.Join(root, "architecture", "concepts.yaml"),
		filepath.Join(root, "architecture", "decisions.yaml"),
		filepath.Join(root, "architecture", "invariants.yaml"),
	}
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		decoder := yaml.NewDecoder(strings.NewReader(string(contents)))
		var document yaml.Node
		if err := decoder.Decode(&document); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		var trailing yaml.Node
		if err := decoder.Decode(&trailing); err != io.EOF {
			t.Fatalf("expected one YAML document in %s, got trailing error %v", path, err)
		}
	}

	baseline := readR01ArchitectureFile(t, filepath.Join(root, "architecture", "baseline.yaml"))
	decisions := readR01ArchitectureFile(t, filepath.Join(root, "architecture", "decisions.yaml"))
	concepts := readR01ArchitectureFile(t, filepath.Join(root, "architecture", "concepts.yaml"))

	for _, required := range []struct{ text, want string }{
		{baseline, "intent_boundary:"},
		{baseline, "implementation_status: \"NOT_IMPLEMENTED\""},
		{baseline, "final_architecture_freeze: \"NOT_CLAIMED\""},
		{decisions, "D-R0.1-INTENT-DUAL-LAYER"},
		{decisions, "partially_supersedes:"},
		{decisions, "current Kernel ObjectReference rejection"},
		{decisions, "interpretation_anchors:"},
		{decisions, "section: \"2.3 Intent Boundary\""},
		{decisions, "section: \"5 Frozen Decisions / item 9\""},
		{decisions, "post_K2_E_impact:"},
		{concepts, "future_kernel_managed_concept"},
		{concepts, "ObjectKindIntent represents neither UserIntent nor current KernelIntent"},
	} {
		if !strings.Contains(required.text, required.want) {
			t.Fatalf("current architecture missing %q", required.want)
		}
	}
	assertCurrentDesignText(t, root, "kernel-userspace-boundary.md", []string{
		"Intent Formulation",
		"Satisfaction Planning",
		"Kernel-managed persistent opaque representation",
		"EvaluationState remains User Space",
		"ObjectKindIntent remains rejected",
		"runtime is NOT_IMPLEMENTED",
		"KernelIntent grants no Capability authority",
		"Contradictory observations and their provenance must remain distinguishable",
		"EvaluationState UNKNOWN is not invocation-outcome UNKNOWN",
		"NOT_IMPLEMENTED is not ARCHITECTURALLY_FORBIDDEN",
	})
	if !strings.Contains(concepts, "status: \"CANDIDATE\"") && !strings.Contains(concepts, "status: \"FROZEN_SEMANTICS_ONLY\"") {
		t.Fatal("future KernelIntent concept must remain a candidate or semantic-only freeze")
	}

	baselineDocument := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "baseline.yaml"))
	decisionsDocument := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "decisions.yaml"))
	conceptsDocument := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", "concepts.yaml"))
	intentBoundary := r01MapField(t, baselineDocument, "intent_boundary")
	state := r01MapField(t, baselineDocument, "state")
	if r01StringField(t, state, "current_review") != "DTM-Kernel-v0.1-Gate-B-Phase-2-Closure-Record" {
		t.Fatalf("architecture state current_review must point to the current Gate B Phase 2 closure record")
	}
	currentPhase := r01MapField(t, baselineDocument, "current_phase")
	currentSubphase := r01MapField(t, currentPhase, "current_subphase")
	if r01StringField(t, currentPhase, "id") != "GATE-B-PHASE-2" || r01StringField(t, currentPhase, "status") != "CLOSED" || r01StringField(t, currentPhase, "design_status") != "ACCEPTED" || r01StringField(t, currentPhase, "review_status") != "PASS" || r01StringField(t, currentPhase, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, currentPhase, "implementation_readiness") != "CLOSED" || r01StringField(t, currentPhase, "implementation_review_status") != "PASS" {
		t.Fatal("architecture state must identify the closed Gate B Phase 2 bounded implementation")
	}
	if r01StringField(t, currentSubphase, "id") != "EXECUTION-RESOLUTION-MECHANISM" || r01StringField(t, currentSubphase, "status") != "CLOSED" || r01StringField(t, currentSubphase, "design_status") != "ACCEPTED" || r01StringField(t, currentSubphase, "review_status") != "PASS" || r01StringField(t, currentSubphase, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, currentSubphase, "implementation_readiness") != "CLOSED" || r01StringField(t, currentSubphase, "implementation_review_status") != "PASS" || r01StringField(t, currentSubphase, "current_review") != "DTM-Kernel-v0.1-Gate-B-Phase-2-Closure-Record" || r01StringField(t, currentSubphase, "decision") != "GATE_B_PHASE_2_CLOSED" {
		t.Fatal("architecture state must identify the closed Gate B Phase 2 execution-resolution subphase")
	}
	if status := r01StringField(t, intentBoundary, "status"); status != "CANDIDATE" && status != "FROZEN_SEMANTICS_ONLY" {
		t.Fatalf("intent_boundary.status = %q, want candidate or semantic-only freeze", status)
	}
	if r01StringField(t, intentBoundary, "implementation_status") != "NOT_IMPLEMENTED" {
		t.Fatal("intent boundary implementation status must remain NOT_IMPLEMENTED")
	}
	readiness := r01MapField(t, baselineDocument, "readiness")
	if r01StringField(t, readiness, "decision") != "GATE_B_PHASE_2_CLOSED" || r01StringField(t, readiness, "status") != "CLOSED" || r01StringField(t, readiness, "design_status") != "ACCEPTED" || r01StringField(t, readiness, "review_status") != "PASS" || r01StringField(t, readiness, "implementation_status") != "BOUNDED_IMPLEMENTED" || r01StringField(t, readiness, "implementation_readiness") != "CLOSED" || r01StringField(t, readiness, "implementation_review_status") != "PASS" || r01StringField(t, readiness, "final_architecture_freeze") != "NOT_CLAIMED" {
		t.Fatal("readiness must record closed Gate B Phase 2 implementation without claiming final Architecture Freeze")
	}

	decision := r01EntryByID(t, decisionsDocument, "decisions", "id", "D-R0.1-INTENT-DUAL-LAYER")
	if r01StringField(t, decision, "supersession_mode") != "PARTIAL" {
		t.Fatal("R0.1 decision must declare PARTIAL supersession")
	}
	if !r01StringListContains(t, decision, "supersedes", "D-INTENT-USERSPACE") {
		t.Fatal("R0.1 decision must explicitly supersede D-INTENT-USERSPACE")
	}
	partial := r01EntryByID(t, decision, "partially_supersedes", "id", "D-R0-INTENT-REFERENCE-BOUNDARY")
	if strings.TrimSpace(r01StringField(t, partial, "scope")) == "" || strings.TrimSpace(r01StringField(t, partial, "retained_aspect")) == "" {
		t.Fatal("R0.1 partial supersession for D-R0-INTENT-REFERENCE-BOUNDARY must include scope and retained_aspect")
	}
	supersessionScope := r01MapField(t, decision, "supersession_scope")
	if r01StringField(t, supersessionScope, "superseded_decision") != "D-INTENT-USERSPACE" || strings.TrimSpace(r01StringField(t, supersessionScope, "retained_aspect")) == "" {
		t.Fatal("R0.1 scoped supersession for D-INTENT-USERSPACE must include its retained aspect")
	}
	anchors := r01ListField(t, decision, "interpretation_anchors")
	if len(anchors) < 2 {
		t.Fatal("R0.1 decision must retain document-level K1 interpretation anchors")
	}
	for _, entry := range r01ListField(t, decision, "partially_supersedes") {
		mapping, ok := entry.(map[string]any)
		if ok && mapping["id"] == "D-INTENT-USERSPACE" {
			t.Fatal("D-INTENT-USERSPACE must use supersession_scope rather than a duplicate partial-supersession edge")
		}
	}

	kernelIntent := r01EntryByID(t, conceptsDocument, "concepts", "ConceptID", "KernelIntent")
	if r01StringField(t, kernelIntent, "semantic_role") != "future_kernel_managed_concept" {
		t.Fatalf("KernelIntent semantic_role = %q, want future_kernel_managed_concept", r01StringField(t, kernelIntent, "semantic_role"))
	}
	if strings.TrimSpace(r01StringField(t, kernelIntent, "future_object_role")) == "" {
		t.Fatal("KernelIntent must state its prospective object role")
	}
	if !strings.Contains(r01StringField(t, kernelIntent, "definition"), "persistent") {
		t.Fatal("KernelIntent definition must identify a persistent representation")
	}
	if r01StringListContains(t, kernelIntent, "minimal_semantics", "EvaluationState") {
		t.Fatal("EvaluationState must not be a required KernelIntent lifecycle semantic")
	}
	persistentDecision := r01EntryByID(t, decisionsDocument, "decisions", "id", "D-R0.1-INTENT-PERSISTENT-REPRESENTATION")
	if r01StringField(t, persistentDecision, "status") != "FROZEN_SEMANTICS_ONLY" || r01StringField(t, persistentDecision, "supersession_mode") != "PARTIAL" {
		t.Fatalf("persistent KernelIntent decision = %#v, want FROZEN_SEMANTICS_ONLY/PARTIAL", persistentDecision)
	}
	persistentPartial := r01EntryByID(t, persistentDecision, "partially_supersedes", "id", "D-R0.1-INTENT-DUAL-LAYER")
	if strings.TrimSpace(r01StringField(t, persistentPartial, "scope")) == "" || strings.TrimSpace(r01StringField(t, persistentPartial, "retained_aspect")) == "" {
		t.Fatal("persistent KernelIntent correction must record superseded scope and retained aspect")
	}

	candidate := strings.Contains(baseline, "decision: \"INTENT_BOUNDARY_CANDIDATE\"")
	accepted := strings.Contains(baseline, "decision: \"INTENT_BOUNDARY_ACCEPTED\"")
	if candidate == accepted {
		t.Fatal("baseline must contain exactly one Intent candidate/accepted decision")
	}
	if !accepted {
		t.Fatal("current Intent semantic boundary must remain accepted; changing governance requires an explicit decision")
	}

	forbiddenKernelSymbols := []string{"Intent" + "Manager", "Intent" + "Store", "Intent" + "Evaluator", "Intent" + "Reconciler", "Intent" + "Scheduler", "Intent" + "TaskGenerator"}
	kernelRoot := filepath.Join(root, "internal", "kernel")
	if err := filepath.WalkDir(kernelRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || filepath.Base(path) == "r0_1_architecture_test.go" {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, symbol := range forbiddenKernelSymbols {
			if strings.Contains(string(contents), symbol) {
				return fmt.Errorf("forbidden future Intent runtime symbol %q found in %s", symbol, path)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func readR01ArchitectureFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(contents)
}

func parseR01ArchitectureYAML(t *testing.T, path string) map[string]any {
	t.Helper()
	contents := readR01ArchitectureFile(t, path)
	decoder := yaml.NewDecoder(strings.NewReader(contents))
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("expected one YAML document in %s, got trailing error %v", path, err)
	}
	return document
}

func r01MapField(t *testing.T, document map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := document[key].(map[string]any)
	if !ok {
		t.Fatalf("YAML field %q is not a mapping", key)
	}
	return value
}

func r01StringField(t *testing.T, document map[string]any, key string) string {
	t.Helper()
	value, ok := document[key].(string)
	if !ok {
		t.Fatalf("YAML field %q is not a string", key)
	}
	return value
}

func r01ListField(t *testing.T, document map[string]any, key string) []any {
	t.Helper()
	value, ok := document[key].([]any)
	if !ok {
		t.Fatalf("YAML field %q is not a list", key)
	}
	return value
}

func r01EntryByID(t *testing.T, document map[string]any, listKey, idKey, id string) map[string]any {
	t.Helper()
	for _, entry := range r01ListField(t, document, listKey) {
		mapping, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if value, ok := mapping[idKey].(string); ok && value == id {
			return mapping
		}
	}
	t.Fatalf("YAML list %q does not contain %s=%q", listKey, idKey, id)
	return nil
}

func r01StringListContains(t *testing.T, document map[string]any, key, want string) bool {
	t.Helper()
	values, ok := document[key].([]any)
	if !ok {
		t.Fatalf("YAML field %q is not a list", key)
	}
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func r01Header(document string) string {
	lines := strings.Split(document, "\n")
	if len(lines) > 10 {
		lines = lines[:10]
	}
	return strings.Join(lines, "\n")
}
