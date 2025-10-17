package ingest

import (
	"net/http"
	// "time"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	basecontroller "github.com/sofc-t/event_flow/src/controller"
	icmd "github.com/sofc-t/event_flow/src/service/cqrs/command"
	iqry "github.com/sofc-t/event_flow/src/service/cqrs/query"
	dto "github.com/sofc-t/event_flow/src/service/ingest/dto"

	"github.com/sofc-t/event_flow/src/observability"
	"go.uber.org/zap"
)

// IngestController handles ingestion-related endpoints.
type IngestController struct {
	basecontroller.BaseController
	createIngestHandler icmd.IHandler[*dto.IngestCommand, bool]
	getIngestHandler    iqry.IHandler[uuid.UUID, *dto.IngestRecord]
	listIngestsHandler  iqry.IHandler[int, []*dto.IngestRecord]
}

// Config holds the configuration dependencies for creating an IngestController.
type Config struct {
	CreateIngestHandler icmd.IHandler[*dto.IngestCommand, bool]
	GetIngestHandler    iqry.IHandler[uuid.UUID, *dto.IngestRecord]
	ListIngestsHandler  iqry.IHandler[int, []*dto.IngestRecord]
}

// New creates a new IngestController with its CQRS handlers.
func NewIngestController(config Config) *IngestController {
	return &IngestController{
		createIngestHandler: config.CreateIngestHandler,
		getIngestHandler:    config.GetIngestHandler,
		listIngestsHandler:  config.ListIngestsHandler,
	}
}

// RegisterPublic registers open routes (no authentication required).
func (i *IngestController) RegisterPublic(router *gin.RouterGroup) {
	router = router.Group("/ingest")
	router.POST("/", i.createIngest)
	router.GET("/:id", i.getIngest)
	router.GET("/page/:page", i.listIngests)
}

// RegisterProtected registers authenticated routes (if any).
func (i *IngestController) RegisterProtected(router *gin.RouterGroup) {
	// Example protected routes can go here later.
}

// RegisterPrivileged registers admin-only or privileged routes.
func (i *IngestController) RegisterPrivileged(router *gin.RouterGroup) {}

// RegisterEmpty satisfies your controller registration interface.
func (i *IngestController) RegisterEmpty(router *gin.RouterGroup) {}

// createIngest handles ingestion of incoming data.
func (i *IngestController) createIngest(ctx *gin.Context) {
	logger := observability.Logger.Ctx(ctx.Request.Context())

	var cmd dto.IngestCommand
	if err := ctx.BindJSON(&cmd); err != nil {
		logger.Error("invalid ingest request", zap.Error(err))
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	// cmd.Timestamp = time.Now()

	logger.Info("processing ingest command: sent to processor", zap.Any("cmd", cmd))
	ok, err := i.createIngestHandler.Handle(&cmd)
	if err != nil {
		logger.Error("failed to handle ingest command", zap.Error(err))
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process ingest"})
		return
	}

	if !ok {
		logger.Warn("ingest command failed validation", zap.Any("cmd", cmd))
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Ingest rejected"})
		return
	}



	logger.Info("ingest created successfully", zap.Any("cmd", cmd))
	ctx.JSON(http.StatusCreated, gin.H{"status": "success"})
}

// getIngest returns an ingest record by ID.
func (i *IngestController) getIngest(ctx *gin.Context) {
	logger := observability.Logger.Ctx(ctx.Request.Context())
	idStr := ctx.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		logger.Error("invalid uuid", zap.String("id", idStr), zap.Error(err))
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}

	record, err := i.getIngestHandler.Handle(id)
	if err != nil {
		logger.Error("failed to fetch ingest", zap.String("id", idStr), zap.Error(err))
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get ingest"})
		return
	}

	ctx.JSON(http.StatusOK, record)
}

// listIngests lists ingests by page.
func (i *IngestController) listIngests(ctx *gin.Context) {
	logger := observability.Logger.Ctx(ctx.Request.Context())
	pageParam := ctx.Param("page")
	page, err := strconv.Atoi(pageParam)
	if err != nil {
		logger.Error("invalid page param", zap.String("page", pageParam), zap.Error(err))
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid page"})
		return
	}

	records, err := i.listIngestsHandler.Handle(page)
	if err != nil {
		logger.Error("failed to list ingests", zap.Error(err))
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list ingests"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"data": records})
}
