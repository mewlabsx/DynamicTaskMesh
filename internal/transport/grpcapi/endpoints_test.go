package grpcapi

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"dtm/internal/model"
)

func TestEndpointDirectorySetAndResolveTrimmedValues(t *testing.T) {
	directory := NewEndpointDirectory()

	if err := directory.Set(model.NodeID(" node-1 "), " 127.0.0.1:50061 "); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	got, err := directory.Resolve(model.NodeID("node-1"))
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got != "127.0.0.1:50061" {
		t.Fatalf("Resolve() = %q, want trimmed endpoint", got)
	}
}

func TestEndpointDirectorySetRejectsInvalidEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		id      model.NodeID
		address string
	}{
		{name: "empty node ID", address: "127.0.0.1:50061"},
		{name: "blank node ID", id: " \t ", address: "127.0.0.1:50061"},
		{name: "empty address", id: "node-1"},
		{name: "blank address", id: "node-1", address: " \t "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewEndpointDirectory().Set(tt.id, tt.address)
			if !errors.Is(err, ErrInvalidEndpoint) {
				t.Fatalf("Set() error = %v, want ErrInvalidEndpoint", err)
			}
		})
	}
}

func TestEndpointDirectoryResolveMissingWrapsNotFound(t *testing.T) {
	_, err := NewEndpointDirectory().Resolve(model.NodeID("missing"))
	if !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("Resolve() error = %v, want ErrEndpointNotFound", err)
	}
}

func TestEndpointDirectorySetReplacesExistingEndpoint(t *testing.T) {
	directory := NewEndpointDirectory()
	if err := directory.Set(model.NodeID("node-1"), "127.0.0.1:50061"); err != nil {
		t.Fatal(err)
	}
	if err := directory.Set(model.NodeID("node-1"), "127.0.0.1:50062"); err != nil {
		t.Fatal(err)
	}

	got, err := directory.Resolve(model.NodeID("node-1"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "127.0.0.1:50062" {
		t.Fatalf("Resolve() = %q, want replacement endpoint", got)
	}
}

func TestEndpointDirectoryInvalidSetPreservesExistingEndpoint(t *testing.T) {
	directory := NewEndpointDirectory()
	if err := directory.Set(model.NodeID("node-1"), "127.0.0.1:50061"); err != nil {
		t.Fatal(err)
	}

	if err := directory.Set(model.NodeID(" node-1 "), " "); !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("Set() error = %v, want ErrInvalidEndpoint", err)
	}

	got, err := directory.Resolve(model.NodeID("node-1"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "127.0.0.1:50061" {
		t.Fatalf("Resolve() = %q, want original endpoint", got)
	}
}

func TestEndpointDirectoryDeleteRemovesEndpoint(t *testing.T) {
	directory := NewEndpointDirectory()
	if err := directory.Set(model.NodeID("node-1"), "127.0.0.1:50061"); err != nil {
		t.Fatal(err)
	}

	directory.Delete(model.NodeID(" node-1 "))

	if _, err := directory.Resolve(model.NodeID("node-1")); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("Resolve() after Delete error = %v, want ErrEndpointNotFound", err)
	}
}

func TestEndpointDirectorySupportsConcurrentReadWrite(t *testing.T) {
	directory := NewEndpointDirectory()
	const goroutines = 32
	const iterations = 100

	var wg sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := model.NodeID(fmt.Sprintf("node-%d", worker))
			for iteration := 0; iteration < iterations; iteration++ {
				if err := directory.Set(id, fmt.Sprintf("127.0.0.1:%d", 50000+iteration)); err != nil {
					t.Errorf("Set() error = %v", err)
					return
				}
				if _, err := directory.Resolve(id); err != nil {
					t.Errorf("Resolve() error = %v", err)
					return
				}
			}
			directory.Delete(id)
		}()
	}
	wg.Wait()
}
