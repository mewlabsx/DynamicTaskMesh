package mapper

import (
	"errors"
	"testing"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

// valueDirectoryReader is a value-type (non-pointer) implementation used to
// prove the nil detection does not reject legitimate value receivers.
type valueDirectoryReader struct {
	views []resourcedirectory.ResourceRecordView
}

func (reader valueDirectoryReader) MatchRequirement(requirement model.ResourceRequirement) ([]resourcedirectory.ResourceRecordView, error) {
	cloned := make([]resourcedirectory.ResourceRecordView, len(reader.views))
	for index := range reader.views {
		cloned[index] = reader.views[index].Clone()
	}
	return cloned, nil
}

// valueReadiness is a value-type (non-pointer) Readiness implementation.
type valueReadiness struct {
	ready bool
}

func (readiness valueReadiness) Ready() bool { return readiness.ready }

// TestNewResourceCandidateSourceNilInterfaceRejected reproduces the M4-A-R1
// defect on the M4-A baseline (be10f25): a true nil interface is not detected
// by the reflect-based nil helpers (reflect.ValueOf(nil).Kind() == Invalid,
// which is not handled), so the constructor wrongly succeeds and the later
// Query would panic on a nil dependency.
//
// BEFORE FIX: this test fails (constructor succeeds for nil dependencies).
func TestNewResourceCandidateSourceNilInterfaceRejected(t *testing.T) {
	tests := []struct {
		name      string
		directory ResourceDirectoryReader
		readiness ResourceCandidateReadiness
	}{
		{name: "nil directory", directory: nil, readiness: &testReadiness{ready: true}},
		{name: "nil readiness", directory: &fakeDirectoryReader{}, readiness: nil},
		{name: "both nil", directory: nil, readiness: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, err := NewResourceCandidateSource(test.directory, test.readiness)
			if err == nil {
				t.Fatal("constructor accepted nil dependencies; Query would panic later")
			}
			if source != nil {
				t.Fatalf("constructor returned non-nil source %#v with error %v", source, err)
			}
			if !errors.Is(err, ErrInvalidResourceCandidateQuery) {
				t.Fatalf("constructor error = %v, want %v", err, ErrInvalidResourceCandidateQuery)
			}
		})
	}
}

// TestNewResourceCandidateSourceTypedNilRejected verifies typed-nil pointer
// dependencies are rejected as well (interface non-nil, dynamic value nil).
func TestNewResourceCandidateSourceTypedNilRejected(t *testing.T) {
	var typedNilReader *fakeDirectoryReader
	var typedNilReadiness *testReadiness
	tests := []struct {
		name      string
		directory ResourceDirectoryReader
		readiness ResourceCandidateReadiness
	}{
		{name: "typed nil directory", directory: typedNilReader, readiness: &testReadiness{ready: true}},
		{name: "typed nil readiness", directory: &fakeDirectoryReader{}, readiness: typedNilReadiness},
		{name: "typed nil both", directory: typedNilReader, readiness: typedNilReadiness},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, err := NewResourceCandidateSource(test.directory, test.readiness)
			if err == nil {
				t.Fatal("constructor accepted typed-nil dependencies; Query would panic later")
			}
			if source != nil {
				t.Fatalf("constructor returned non-nil source %#v with error %v", source, err)
			}
			if !errors.Is(err, ErrInvalidResourceCandidateQuery) {
				t.Fatalf("constructor error = %v, want %v", err, ErrInvalidResourceCandidateQuery)
			}
		})
	}
}

// TestNewResourceCandidateSourceValidDependencies verifies pointer and
// value-type implementations are both accepted and query correctly.
func TestNewResourceCandidateSourceValidDependencies(t *testing.T) {
	t.Run("pointer implementations", func(t *testing.T) {
		view := candidateView(t, "resource-a", "node-a", 1, "registration-a", 1)
		reader := &fakeDirectoryReader{views: []resourcedirectory.ResourceRecordView{view}}
		source, err := NewResourceCandidateSource(reader, &testReadiness{ready: true})
		if err != nil {
			t.Fatal(err)
		}
		got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
		if err != nil || len(got) != 1 || got[0].Descriptor.ID != "resource-a" {
			t.Fatalf("Query() = %#v, %v", got, err)
		}
	})
	t.Run("value implementations", func(t *testing.T) {
		view := candidateView(t, "resource-b", "node-b", 1, "registration-b", 1)
		reader := valueDirectoryReader{views: []resourcedirectory.ResourceRecordView{view}}
		source, err := NewResourceCandidateSource(reader, valueReadiness{ready: true})
		if err != nil {
			t.Fatal(err)
		}
		got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
		if err != nil || len(got) != 1 || got[0].Descriptor.ID != "resource-b" {
			t.Fatalf("Query() = %#v, %v", got, err)
		}
	})
	t.Run("value reader with pointer readiness", func(t *testing.T) {
		view := candidateView(t, "resource-c", "node-c", 1, "registration-c", 1)
		reader := valueDirectoryReader{views: []resourcedirectory.ResourceRecordView{view}}
		source, err := NewResourceCandidateSource(reader, &testReadiness{ready: true})
		if err != nil {
			t.Fatal(err)
		}
		got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
		if err != nil || len(got) != 1 || got[0].Descriptor.ID != "resource-c" {
			t.Fatalf("Query() = %#v, %v", got, err)
		}
	})
}
