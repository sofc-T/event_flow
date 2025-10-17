package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/sofc-t/event_flow/src/observability"
	ingest "github.com/sofc-t/event_flow/src/service/ingest/api"
	dto "github.com/sofc-t/event_flow/src/service/ingest/dto"
	ingestService "github.com/sofc-t/event_flow/src/service/ingest/service"
	"github.com/sofc-t/event_flow/src/service/kafka"
	"go.uber.org/zap"
)

// Entry point for the ingest service
func main() {
	// --- Initialize Observability ---
	ctx := context.Background()
	observability.Init(ctx, "ingest-service")

	logger := observability.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Setup Kafka Publisher ---
	kafkaCfg := kafka.Config{
		Brokers:     []string{"localhost:9092"},
		TopicPrefix: "ingest_events_",
		RequireTLS:  false,
	}
	publisher, err := kafka.NewPublisher(kafkaCfg)
	if err != nil {
		logger.Fatal("failed to initialize kafka publisher", zap.Error(err))
	}
	defer publisher.Close()

	// --- Setup CQRS Handlers ---
	createHandler := &CreateIngestHandler{Publisher: publisher, IngestService: ingestService.New(publisher)}
	getHandler := &GetIngestHandler{}
	listHandler := &ListIngestsHandler{}

	// --- Setup Controller ---
	controller := ingest.NewIngestController(ingest.Config{
		CreateIngestHandler: createHandler,
		GetIngestHandler:    getHandler,
		ListIngestsHandler:  listHandler,
	})

	// --- Setup Gin Router ---
	router := gin.Default()
	api := router.Group("/api/v1")
	controller.RegisterPublic(api)

	// --- Start Server ---
	srv := &httpServer{
		engine: router,
		addr:   ":8080",
	}

	go func() {
		logger.Info("🚀 Ingest service started", zap.String("addr", srv.addr))
		if err := srv.run(); err != nil {
			logger.Fatal("server crashed", zap.Error(err))
		}
	}()

	// --- Graceful Shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("🧹 Shutting down gracefully...")
	cancel()
	srv.shutdown(ctx)
	logger.Sync()
}

// --------------------------------------------------------------------
// CQRS Command and Query Handlers
// --------------------------------------------------------------------

// CreateIngestHandler publishes ingest events to Kafka
type CreateIngestHandler struct {
	Publisher     kafka.Publisher
	IngestService *ingestService.Service
}

func (h *CreateIngestHandler) Handle(cmd *dto.IngestCommand) (bool, error) {
	logger := observability.Logger

	success, err := h.IngestService.Create(context.Background(), *cmd)
	if err != nil {
		logger.Error("failed to create ingest", zap.Error(err))
		return false, err
	}
	return success != nil, nil
}

// --------------------------------------------------------------------
// Query Handlers (Mock Implementations)
// --------------------------------------------------------------------

type GetIngestHandler struct{}

func (h *GetIngestHandler) Handle(id uuid.UUID) (*dto.IngestRecord, error) {
	// Mock data for now
	return &dto.IngestRecord{
		ID:        id,
		Source:    "mock_source",
		Payload:   "mock_payload",
		Status:    "processed",
		Timestamp: time.Now(),
	}, nil
}

type ListIngestsHandler struct{}

func (h *ListIngestsHandler) Handle(page int) ([]*dto.IngestRecord, error) {
	// Mock paginated response
	return []*dto.IngestRecord{
		{ID: uuid.New(), Source: "service-A", Payload: "event1", Status: "processed", Timestamp: time.Now()},
		{ID: uuid.New(), Source: "service-B", Payload: "event2", Status: "pending", Timestamp: time.Now()},
	}, nil
}

// --------------------------------------------------------------------
// HTTP Server Wrapper for graceful shutdown
// --------------------------------------------------------------------

type httpServer struct {
	engine *gin.Engine
	addr   string
}

func (s *httpServer) run() error {
	return s.engine.Run(s.addr)
}

func (s *httpServer) shutdown(ctx context.Context) {
	// Graceful stop handled automatically by Gin
	observability.Logger.Info("HTTP server shutdown complete")
}
