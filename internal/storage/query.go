package storage

import (
	"time"

	"dtm/internal/lifecycle"
)

const (
	DefaultTaskPageLimit = 50
	MaxTaskPageLimit     = 200
)

// TaskFilter describes the stable, cursor-based task listing contract.
// CreatedAfter and CreatedBefore are exclusive UTC boundaries.
type TaskFilter struct {
	Status        *lifecycle.State
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Limit         int
	PageToken     string
}

type TaskPage struct {
	Tasks         []Task
	NextPageToken string
}
