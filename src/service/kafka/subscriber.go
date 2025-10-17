package kafka

import (
	"context"
	"time"
	k "github.com/segmentio/kafka-go"
	"github.com/sofc-t/event_flow/src/observability"
	"go.uber.org/zap"
)


type subscriber struct {
	reader *k.Reader
	logger *zap.Logger
}

// NewSubscriber creates a new Kafka subscriber.
func NewSubscriber(cfg Config) (Subscriber, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = observability.Logger.Logger
	}

	readerCfg := k.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: cfg.GroupID,
		MaxWait: 500 * time.Millisecond,
	}

	s := &subscriber{
		reader: k.NewReader(readerCfg),
		logger: logger,
	}

	logger.Info("✅ Kafka subscriber initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("group_id", cfg.GroupID),
	)
	return s, nil
}

// Subscribe consumes messages continuously.
func (s *subscriber) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	s.reader.SetOffset(k.LastOffset)
	s.logger.Info("📥 Subscribing to topic", zap.String("topic", topic))

	for {
		msg, err := s.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				s.logger.Info("🛑 Subscription stopped", zap.String("topic", topic))
				return nil
			}
			s.logger.Error("⚠️ Failed to read message", zap.Error(err))
			continue
		}

		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			s.logger.Error("❌ Handler failed", zap.Error(err))
		}
	}
}

func (s *subscriber) Close() error {
	return s.reader.Close()
}
