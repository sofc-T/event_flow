package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	// "github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	"github.com/sofc-t/event_flow/src/service/process/dto"
	repo "github.com/sofc-t/event_flow/src/service/process/repo"
)

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroupID string

	DBHost string
	DBPort string
	DBUser string
	DBPass string
	DBName string
}

func loadConfig() Config {
	return Config{
		KafkaBrokers: getenv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getenv("KAFKA_TOPIC", "ingest_events"),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "process_group"),

		DBHost: getenv("DB_HOST", "localhost"),
		DBPort: getenv("DB_PORT", "5432"),
		DBUser: getenv("DB_USER", "flow"),
		DBPass: getenv("DB_PASS", "flow123"),
		DBName: getenv("DB_NAME", "flowdb"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log.Println("[Process] Starting process service...")
	cfg := loadConfig()

	// ---- DB Setup ----
	connStr := "host=" + cfg.DBHost +
		" port=" + cfg.DBPort +
		" user=" + cfg.DBUser +
		" password=" + cfg.DBPass +
		" dbname=" + cfg.DBName +
		" sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("❌ failed to connect to DB: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("❌ cannot reach DB: %v", err)
	}
	log.Println("✅ Connected to PostgreSQL")

	repository := repo.NewProcessRepo(db)

	// ---- Kafka Setup ----
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBrokers},
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})
	defer reader.Close()

	log.Printf("✅ Kafka consumer ready on topic=%s", cfg.KafkaTopic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("⚠️ Kafka read error: %v", err)
				continue
			}

			event := dto.ProcessEvent{
				ID:        uuid.NewString(),
				Timestamp: time.Now(),
				Payload:   string(msg.Value),
				Source:    string(msg.Key),
			}

			if err := repository.SaveEvent(ctx, &event); err != nil {
				log.Printf("❌ failed to save event: %v", err)
			} else {
				log.Printf("💾 saved event from key=%s", msg.Key)
			}
		}
	}()

	// ---- Graceful Shutdown ----
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("🛑 shutting down gracefully...")
	cancel()
	reader.Close()
	db.Close()
	log.Println("✅ process service stopped")
}
