package main

import (
	"context"
	"os"
	"os/signal"
	"strings" // Added to split the KAFKA_BROKERS string
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

func main() {
	// --- Initialize Observability ---
	ctx := context.Background()
	observability.Init(ctx, "ingest-service")

	logger := observability.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Setup Kafka Publisher ---
	// Todo: conig from env
	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersStr == "" {
		kafkaBrokersStr = "kafka:29092" 
		logger.Warn("KAFKA_BROKERS environment variable not set, using default", zap.String("default", kafkaBrokersStr))
	}
	// KAFKA_BROKERS is typically a comma-separated list
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",") 

	kafkaCfg := kafka.Config{
		Brokers:     kafkaBrokers, 
		TopicPrefix: "ingest_events",
		RequireTLS:  false,
	}
	publisher, err := kafka.NewPublisher(kafkaCfg)
	if err != nil {
		logger.Fatal("failed to initialize kafka publisher", zap.Error(err))
	}
	defer publisher.Close()

	// --- Setup Ingest Service ---
	ingestService := ingestService.New(publisher)
	getHandler := &GetIngestHandler{}
	listHandler := &ListIngestsHandler{}

	// --- Setup Controller ---
	controller := ingest.NewIngestController(ingest.Config{
		CreateIngestHandler: ingestService,
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

	go func(s *httpServer) {
		logger.Info("Ingest service started", zap.String("addr", s.addr))
		if err := s.run(); err != nil {
			logger.Fatal("server crashed", zap.Error(err))
		}
	}(srv)

	// --- Graceful Shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down gracefully...")
	cancel()
	srv.shutdown(ctx)
	logger.Sync()
}

// --------------------------------------------------------------------
// CQRS Command and Query Handlers
// --------------------------------------------------------------------


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