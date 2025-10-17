package kafka

import (
	"context"
	"crypto/tls"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/sofc-t/event_flow/src/observability"
	"go.uber.org/zap"
)


type publisher struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewPublisher creates a Kafka writer suitable for production.
func NewPublisher(cfg Config) (Publisher, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = observability.Logger.Logger
	}

	// --- Transport setup ---
	var tlsConfig *tls.Config
	if cfg.RequireTLS {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	transport := &kafka.Transport{
		TLS: tlsConfig,
		// SASL authentication if needed:
		// SASL: plain.Mechanism{Username: cfg.SASLUser, Password: cfg.SASLPassword},
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Transport:              transport,
		Balancer:               &kafka.LeastBytes{},
		Compression:            cfg.Compression,
		BatchBytes:             cfg.BatchBytes,
		BatchTimeout:           cfg.BatchTimeout,
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
		Async:                  cfg.Async,
	}

	logger.Info("Kafka publisher initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.Bool("tls_enabled", cfg.RequireTLS),
		zap.Bool("async", cfg.Async),
	)

	return &publisher{writer: writer, logger: logger}, nil
}

// Publish sends a message with basic retry logic.
func (p *publisher) Publish(ctx context.Context, topic, key string, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}

	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = p.writer.WriteMessages(ctxTimeout, msg)
		cancel()

		if err == nil {
			p.logger.Debug(" Message published",
				zap.String("topic", topic),
				zap.String("key", key),
			)
			return nil
		}

		p.logger.Warn(" Retry publishing message",
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}

	p.logger.Error(" Failed to publish message after retries", zap.Error(err))
	return err
}

func (p *publisher) Close() error {
	return p.writer.Close()
}
