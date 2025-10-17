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
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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

// Init sets up structured logging, tracing, metrics, and context propagation.
// Returns a shutdown function you should defer in main().
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	// --- Setup Zap Logger ---
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.TimeKey = "timestamp"

	jsonEncoder := zapcore.NewJSONEncoder(encoderCfg)
	stdout := zapcore.AddSync(os.Stdout)
	core := zapcore.NewCore(jsonEncoder, stdout, zapcore.InfoLevel)
	baseZap := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// --- Read OTLP Endpoint from env (default localhost:4318) ---
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4318"
	}

	// --- Setup OpenTelemetry Exporters ---
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		baseZap.Warn("Failed to create OTLP trace exporter, continuing with logging only", zap.Error(err))
		Logger = otelzap.New(baseZap)
		return func(ctx context.Context) error { return nil }, nil
	}

	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		baseZap.Warn("Failed to create OTLP metric exporter", zap.Error(err))
	}

	// --- Create Resource Attributes ---
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(os.Getenv("SERVICE_VERSION")),
			semconv.DeploymentEnvironmentKey.String(os.Getenv("ENV")),
		),
	)
	if err != nil {
		baseZap.Error("Failed to create resource", zap.Error(err))
		return nil, err
	}

	// --- Tracer Provider ---
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	// --- Metric Provider ---
	var mp *sdkmetric.MeterProvider
	if metricExporter != nil {
		mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
			sdkmetric.WithResource(res),
		)
		otel.SetMeterProvider(mp)
	}

	// --- Set Global Providers ---
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		baseZap.Error("OpenTelemetry internal error", zap.Error(err))
	}))

	// --- Setup Global Logger ---
	Logger = otelzap.New(baseZap)
	Logger.Info("✅ Observability initialized",
		zap.String("service", serviceName),
		zap.String("otlp_endpoint", endpoint),
	)

	// --- Graceful shutdown ---
	shutdown := func(ctx context.Context) error {
		var firstErr error
		if err := tp.Shutdown(ctx); err != nil {
			firstErr = fmt.Errorf("trace shutdown: %w", err)
		}
		if mp != nil {
			if err := mp.Shutdown(ctx); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("metric shutdown: %w", err)
			}
		}
		return firstErr
	}

	return shutdown, nil
}

//
// -----------------------------
// HTTP HELPERS
// -----------------------------

// NewHTTPHandler wraps an http.Handler with OpenTelemetry tracing and context propagation.
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
// gRPC HELPERS
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
		grpc.WithTransportCredentials(insecure.NewCredentials()), // change for TLS if needed
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	return grpc.Dial(target, opts...)
}
