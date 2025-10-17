package kafka

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
	"time"
	"crypto/tls"
	"go.uber.org/zap"

)

// MessageHandler is called for each consumed message.
// Return error if processing failed (caller will log it).
type MessageHandler func(ctx context.Context, key, value []byte, ) error


// Publisher publishes messages to Kafka.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
	Close() error
}

// Subscriber subscribes to topics and dispatches messages to handlers.
type Subscriber interface {
	// Subscribe starts consuming messages from topic and runs handler for each message.
	// It should return when ctx is cancelled or an unrecoverable error occurs.
	Subscribe(ctx context.Context, topic string, handler MessageHandler) error
	Close() error
}


// Config defines Kafka connection and security settings.
type Config struct {
	Brokers        []string
	TopicPrefix    string
	ClientID       string
	GroupID        string
	Compression    kafka.Compression
	BatchBytes     int64
	BatchTimeout   time.Duration
	RequireTLS     bool
	TLSConfig      *tls.Config
	SASLUser       string
	SASLPassword   string
	SASLMechanism  string // e.g. "PLAIN", "SCRAM-SHA-256"
	RetryMax       int
	AckPolicy      kafka.RequiredAcks
	Async          bool
	MetricsEnabled bool
	Logger         *zap.Logger
}


// CompressionType is a thin wrapper to allow easier configuration.
// We'll map it to kafka.Compression values in code.
type CompressionType int

const (
	CompressionNone CompressionType = iota
	CompressionGZIP
	CompressionSnappy
)
