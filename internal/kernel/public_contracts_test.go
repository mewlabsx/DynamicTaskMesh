package kernel_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// Current public contracts are required inputs, never optional historical fixtures.
func assertCurrentDesignText(t *testing.T, root, topic string, required []string) {
	t.Helper()
	contents := readR01ArchitectureFile(t, filepath.Join(root, "docs", topic))
	normalized := strings.Join(strings.Fields(contents), " ")
	for _, want := range required {
		if !strings.Contains(normalized, want) {
			t.Fatalf("current contract %s missing %q", topic, want)
		}
	}
}

func TestCurrentArchitectureDocumentReferences(t *testing.T) {
	root := filepath.Join("..", "..")
	files := []string{"baseline.yaml", "boundaries.yaml", "concepts.yaml", "decisions.yaml", "invariants.yaml"}
	for _, file := range files {
		document := parseR01ArchitectureYAML(t, filepath.Join(root, "architecture", file))
		checkCurrentDocumentReferences(t, root, file, document)
	}
}

var currentDocumentReference = regexp.MustCompile(`(?:docs/|architecture/)[A-Za-z0-9_./-]+\.md(?:#[^"\n]+)?|(?:README|AGENTS)\.md(?:#[^"\n]+)?`)

func checkCurrentDocumentReferences(t *testing.T, root, location string, value any) {
	t.Helper()
	switch item := value.(type) {
	case map[string]any:
		for key, child := range item {
			checkCurrentDocumentReferences(t, root, location+"."+key, child)
		}
	case []any:
		for _, child := range item {
			checkCurrentDocumentReferences(t, root, location, child)
		}
	case string:
		for _, ref := range currentDocumentReference.FindAllString(item, -1) {
			parts := strings.SplitN(ref, "#", 2)
			contents := readR01ArchitectureFile(t, filepath.Join(root, filepath.FromSlash(parts[0])))
			if len(parts) == 2 {
				wanted := publicHeadingAnchor(parts[1])
				found := false
				for _, line := range strings.Split(contents, "\n") {
					if strings.HasPrefix(line, "#") && publicHeadingAnchor(strings.TrimLeft(line, "# ")) == wanted {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s: missing heading %s", location, ref)
				}
			}
		}
	}
}

func publicHeadingAnchor(value string) string {
	var result strings.Builder
	for _, ch := range strings.ToLower(strings.TrimSpace(value)) {
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '-' {
			result.WriteRune(ch)
		} else if ch == ' ' {
			result.WriteRune('-')
		}
	}
	return result.String()
}

func TestPublicDesignScopeHasNoHistoricalTree(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("..", "..", "docs"))
	if err != nil {
		t.Fatal(err)
	}
	topics := map[string]bool{}
	for _, name := range []string{"README", "quickstart", "examples", "overview", "status", "contracts", "task-model", "kernel-userspace-boundary", "execution-contract", "resource-authority", "recovery-idempotency", "protocol-generation", "validation", "release-readiness", "integration", "history"} {
		topics[name] = true
	}
	if len(entries) != 32 {
		t.Fatalf("public docs contains %d files, want 16 bilingual topics", len(entries))
	}
	for _, entry := range entries {
		name := strings.TrimSuffix(strings.TrimSuffix(entry.Name(), ".md"), ".zh-CN")
		if entry.IsDir() || !topics[name] || !strings.HasSuffix(entry.Name(), ".md") {
			t.Errorf("unexpected public design item %s", entry.Name())
		}
	}
	for name := range topics {
		for _, suffix := range []string{".md", ".zh-CN.md"} {
			readR01ArchitectureFile(t, filepath.Join("..", "..", "docs", name+suffix))
		}
	}
}
