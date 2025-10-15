package zaplogger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)


type Logger struct {
	*zap.Logger
}	

// New creates a structured production-ready logger.
// Writes to both stdout and a rolling file (optional).
func New() (*zap.Logger, error) {
	// Encoder config
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.TimeKey = "timestamp"

	// Create JSON encoder
	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)

	// Output targets
	stdout := zapcore.AddSync(os.Stdout)

	var cores []zapcore.Core
	cores = append(cores, zapcore.NewCore(jsonEncoder, stdout, zapcore.InfoLevel))
	core := zapcore.NewTee(cores...)

	logger := zap.New(core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return logger, nil
}
