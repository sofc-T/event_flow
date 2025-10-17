package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sofc-t/event_flow/src/observability"
	kafka "github.com/sofc-t/event_flow/src/service/kafka"
	dto "github.com/sofc-t/event_flow/src/service/ingest/dto"
	"go.uber.org/zap"
)

// Service handles ingestion commands and queries for the API endpoints.
type Service struct {
	publisher kafka.Publisher
}

// New creates a new Service with a Kafka publisher.
func New(pub kafka.Publisher) *Service {
	return &Service{
		publisher: pub,
	}
}

// Create publishes a new ingest command to Kafka.
func (s *Service) Create(ctx context.Context, cmd dto.IngestCommand) (*dto.IngestResult, error) {
	logger := observability.Logger.Ctx(ctx)

	if cmd.ID == "" {
		cmd.ID = uuid.NewString()
	}
	if cmd.Timestamp.IsZero() {
		cmd.Timestamp = time.Now()
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		logger.Error("failed to marshal ingest command", zap.Error(err))
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	if err := s.publisher.Publish(ctx, cmd.Topic, cmd.ID, data); err != nil {
		logger.Error("failed to publish ingest event", zap.Error(err))
		return nil, fmt.Errorf("publish command: %w", err)
	}

	logger.Info("ingest command published", zap.String("topic", cmd.Topic), zap.String("key", cmd.ID))
	return &dto.IngestResult{
		ID:      cmd.ID,
		Status:  "success",
		Message: "Ingest created successfully",
	}, nil
}

// Get returns an ingest record by ID.
func (s *Service) Get(ctx context.Context, id string) (*dto.IngestRecord, error) {
	logger := observability.Logger.Ctx(ctx)

	uid, err := uuid.Parse(id)
	if err != nil {
		logger.Error("invalid UUID", zap.Error(err))
		return nil, fmt.Errorf("invalid id: %w", err)
	}

	// Mock record
	record := &dto.IngestRecord{
		ID:        uid,
		Source:    "mock_source",
		Payload:   "mock_payload",
		Status:    "processed",
		Timestamp: time.Now(),
	}

	return record, nil
}

// List returns a paginated list of ingest records.
func (s *Service) List(ctx context.Context, page int) ([]*dto.IngestRecord, error) {
	if page < 1 {
		page = 1
	}

	// Mock paginated results
	records := []*dto.IngestRecord{
		{
			ID:        uuid.New(),
			Source:    "source1",
			Payload:   "payload1",
			Status:    "processed",
			Timestamp: time.Now(),
		},
		{
			ID:        uuid.New(),
			Source:    "source2",
			Payload:   "payload2",
			Status:    "processed",
			Timestamp: time.Now(),
		},
	}

	return records, nil
}
