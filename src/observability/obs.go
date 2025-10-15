package observability

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Global observability objects
var (
	Logger *otelzap.Logger
)

// Init sets up structured logging, tracing, and propagation.
// Returns a shutdown func you should defer in main().
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	// --- Setup zap ---
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.TimeKey = "timestamp"

	jsonEncoder := zapcore.NewJSONEncoder(encoderCfg)
	stdout := zapcore.AddSync(os.Stdout)
	core := zapcore.NewCore(jsonEncoder, stdout, zapcore.InfoLevel)

	baseZap := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// --- Setup OpenTelemetry ---
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(os.Getenv("SERVICE_VERSION")),
			semconv.DeploymentEnvironmentKey.String(os.Getenv("ENV")),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// --- Setup propagation (trace context + baggage) ---
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	Logger = otelzap.New(baseZap)
	Logger.Info("Observability initialized", zap.String("service", serviceName))

	return tp.Shutdown, nil
}

//
// -----------------------------
// HTTP HELPERS
// -----------------------------

// NewHTTPHandler wraps an http.Handler with OpenTelemetry tracing and context propagation.
// Use in servers to auto-create spans per request.
func NewHTTPHandler(handler http.Handler, name string) http.Handler {
	return otelhttp.NewHandler(handler, name)
}

// NewHTTPClient returns an http.Client with tracing propagation enabled.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}

//
// -----------------------------
// gRPC HELPERS (new API)
// -----------------------------

// GRPCServer creates a new gRPC server with automatic OTel tracing via StatsHandler.
func GRPCServer(opts ...grpc.ServerOption) *grpc.Server {
	opts = append(opts,
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	return grpc.NewServer(opts...)
}

// GRPCClientConn dials a gRPC server with OpenTelemetry instrumentation (client-side tracing).
func GRPCClientConn(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append(opts,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // remove if using TLS
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	return grpc.Dial(target, opts...)
}
