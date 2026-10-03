package lease

import (
	"errors"
	"testing"
	"time"
)

func TestManagerRegistersAndRenewsLeaseUsingCoreTime(t *testing.T) {
	manager, err := NewManager(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Unix(100, 0)

	registered, err := manager.Register("node-1", "registration-1", started)
	if err != nil {
		t.Fatal(err)
	}
	if !registered.ExpiresAt.Equal(started.Add(5 * time.Second)) {
		t.Fatalf("registered expiry = %v", registered.ExpiresAt)
	}

	heartbeatAt := started.Add(3 * time.Second)
	renewed, err := manager.Heartbeat("node-1", "registration-1", heartbeatAt)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.LastSeen.Equal(heartbeatAt) || !renewed.ExpiresAt.Equal(heartbeatAt.Add(5*time.Second)) {
		t.Fatalf("renewed lease = %+v", renewed)
	}
	if got := manager.Expired(started.Add(6 * time.Second)); len(got) != 0 {
		t.Fatalf("Expired() = %v, want renewed lease online", got)
	}
}

func TestManagerRejectsStaleRegistrationAfterReplacement(t *testing.T) {
	manager, _ := NewManager(time.Second)
	now := time.Unix(100, 0)
	if _, err := manager.Register("node-1", "old", now); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Register("node-1", "new", now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	if _, err := manager.Heartbeat("node-1", "old", now.Add(time.Second)); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("old Heartbeat() error = %v, want ErrStaleRegistration", err)
	}
	if err := manager.Remove("node-1", "old"); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("old Remove() error = %v, want ErrStaleRegistration", err)
	}
	current, exists := manager.Current("node-1")
	if !exists || current.RegistrationID != "new" {
		t.Fatalf("Current() = %+v/%v, want replacement", current, exists)
	}
}

func TestManagerReportsExpiryWithoutRemovingCurrentLease(t *testing.T) {
	manager, _ := NewManager(2 * time.Second)
	now := time.Unix(100, 0)
	_, _ = manager.Register("node-b", "registration-b", now)
	_, _ = manager.Register("node-a", "registration-a", now)

	if got := manager.Expired(now.Add(time.Second)); len(got) != 0 {
		t.Fatalf("Expired() before TTL = %v", got)
	}
	got := manager.Expired(now.Add(2 * time.Second))
	if len(got) != 2 || got[0].NodeID != "node-a" || got[1].NodeID != "node-b" {
		t.Fatalf("Expired() = %v, want stable node order", got)
	}
	if _, exists := manager.Current("node-a"); !exists {
		t.Fatal("Expired() removed lease before registry transition")
	}
}

func TestManagerValidUsesLeaseExpiryAsExclusiveBoundary(t *testing.T) {
	manager, _ := NewManager(2 * time.Second)
	now := time.Unix(100, 0)
	_, _ = manager.Register("node-1", "registration-1", now)

	if !manager.Valid("node-1", now.Add(2*time.Second-time.Nanosecond)) {
		t.Fatal("Valid() before expiry = false")
	}
	if manager.Valid("node-1", now.Add(2*time.Second)) {
		t.Fatal("Valid() at expiry = true")
	}
	if manager.Valid("missing", now) {
		t.Fatal("Valid() for missing node = true")
	}
}

func TestManagerValidatesInputsAndMissingLease(t *testing.T) {
	if _, err := NewManager(0); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("NewManager(0) error = %v", err)
	}
	manager, _ := NewManager(time.Second)
	if _, err := manager.Register("", "registration", time.Now()); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := manager.Heartbeat("missing", "registration", time.Now()); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Heartbeat() error = %v", err)
	}
}
