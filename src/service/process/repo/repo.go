package processrepo

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/sofc-t/event_flow/src/observability"
	"github.com/sofc-t/event_flow/src/service/process/dto"
	_ "github.com/lib/pq"
)

// Repo defines the repository for processing ingested events.
type Repo struct {
	db       *sql.DB
	reader   *kafka.Reader
	brokers  []string
	groupID  string
	topic    string
	cancelFn context.CancelFunc
}

func NewProcessRepo(db *sql.DB) *Repo {
	return &Repo{db: db}
}


// New creates a new process repository for consuming Kafka and persisting events.
func New(db *sql.DB, brokers []string, topic, groupID string) *Repo {
	return &Repo{
		db:      db,
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
	}
}

// StartConsumer starts listening to the Kafka topic and saves messages into the database.
func (r *Repo) StartConsumer(ctx context.Context) {
	logger := observability.Logger.Ctx(ctx)
	ctx, cancel := context.WithCancel(ctx)
	r.cancelFn = cancel

	r.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     r.brokers,
		GroupID:     r.groupID,
		Topic:       r.topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	})

	logger.Info("Kafka consumer started",
		zap.String("topic", r.topic),
		zap.String("group_id", r.groupID),
		zap.Strings("brokers", r.brokers),
	)

	go func() {
		for {
			m, err := r.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					logger.Info("Kafka consumer shutting down gracefully")
					return
				}
				logger.Error("Kafka fetch error", zap.Error(err))
				continue
			}

			var event dto.ProcessEvent
			if err := json.Unmarshal(m.Value, &event); err != nil {
				logger.Error("Failed to unmarshal Kafka message", zap.Error(err))
				continue
			}

			if event.ID == "" {
				event.ID = uuid.NewString()
			}
			if event.Timestamp.IsZero() {
				event.Timestamp = time.Now()
			}

			if err := r.SaveEvent(ctx, &event); err != nil {
				logger.Error("Failed to save event", zap.Error(err))
				continue
			}

			if err := r.reader.CommitMessages(ctx, m); err != nil {
				logger.Error("Failed to commit message", zap.Error(err))
			}

			logger.Info("Processed event successfully",
				zap.String("event_id", event.ID),
				zap.String("source", event.Source),
			)
		}
	}()
}

// SaveEvent inserts or updates a processed event in the SQL database.
func (r *Repo) SaveEvent(ctx context.Context, event *dto.ProcessEvent) error {
	logger := observability.Logger.Ctx(ctx)

	query := `
	INSERT INTO processed_events (id, source, payload, status, created_at)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (id)
	DO UPDATE SET 
		source = EXCLUDED.source, 
		payload = EXCLUDED.payload, 
		status = EXCLUDED.status, 
		created_at = EXCLUDED.created_at;
	`

	_, err := r.db.ExecContext(ctx, query,
		event.ID,
		event.Source,
		event.Payload,
		event.Status,
		event.Timestamp,
	)
	if err != nil {
		logger.Error("Failed to insert event", zap.Error(err))
		return err
	}

	logger.Debug("Event saved to database", zap.String("event_id", event.ID))
	return nil
}

// GetEventByID retrieves an event by its ID.
func (r *Repo) GetEventByID(ctx context.Context, id uuid.UUID) (*dto.ProcessEvent, error) {
	logger := observability.Logger.Ctx(ctx)

	query := `
	SELECT id, source, payload, status, created_at 
	FROM processed_events 
	WHERE id = $1;
	`
	row := r.db.QueryRowContext(ctx, query, id)

	var event dto.ProcessEvent
	if err := row.Scan(&event.ID, &event.Source, &event.Payload, &event.Status, &event.Timestamp); err != nil {
		if err == sql.ErrNoRows {
			logger.Warn("Event not found", zap.String("event_id", id.String()))
			return nil, nil
		}
		logger.Error("Failed to get event", zap.Error(err))
		return nil, err
	}

	return &event, nil
}

// GetEvents retrieves paginated events from the database.
func (r *Repo) GetEvents(ctx context.Context, page int) ([]*dto.ProcessEvent, error) {
	logger := observability.Logger.Ctx(ctx)

	const limit = 20
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * limit

	query := `
	SELECT id, source, payload, status, created_at
	FROM processed_events
	ORDER BY created_at DESC
	LIMIT $1 OFFSET $2;
	`

	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		logger.Error("Failed to query events", zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var events []*dto.ProcessEvent
	for rows.Next() {
		var event dto.ProcessEvent
		if err := rows.Scan(&event.ID, &event.Source, &event.Payload, &event.Status, &event.Timestamp); err != nil {
			logger.Error("Failed to scan event row", zap.Error(err))
			continue
		}
		events = append(events, &event)
	}

	logger.Debug("Fetched events", zap.Int("count", len(events)), zap.Int("page", page))
	return events, nil
}

// Stop gracefully closes the Kafka reader and cancels context.
func (r *Repo) Stop() error {
	logger := observability.Logger

	if r.cancelFn != nil {
		r.cancelFn()
	}
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			logger.Error("Error closing Kafka reader", zap.Error(err))
			return err
		}
		logger.Info("Kafka reader closed")
	}
	return nil
}
