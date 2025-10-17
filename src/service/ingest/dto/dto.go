package dto

import (
	"time"
	
	"github.com/google/uuid"
)


// IngestCommand represents the incoming ingestion request.
type IngestCommand struct {
	ID        string    `json:"id"`
	Source    string    `json:"source"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
	Topic    string    `json:"topic"`
	
}

// IngestRecord represents a persisted or processed ingestion entry.
type IngestRecord struct {
	ID        uuid.UUID    `json:"id"`
	Source    string    `json:"source"`
	Payload   string    `json:"payload"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type IngestResult struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Message string `json:"message,omitempty"`
}


type IngestQuery struct {
	ID string
}