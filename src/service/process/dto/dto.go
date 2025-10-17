package dto

import (
	"time"
)

// ProcessEvent represents an event received from Kafka
// and persisted in the SQL database.
type ProcessEvent struct {
	ID        string    `db:"id"`
	Source    string    `db:"source"`
	Payload   string    `db:"payload"`
	Status    string    `db:"status"`
	Timestamp time.Time `db:"created_at"`
}

// NewProcessEvent creates a new ProcessEvent instance safely.
func NewProcessEvent(source, payload, status string) *ProcessEvent {
	return &ProcessEvent{
		Source:    source,
		Payload:   payload,
		Status:    status,
		Timestamp: time.Now(),
	}
}
