package mq

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/gonflix/no24/api-server/internal/sse"
	"github.com/segmentio/kafka-go"
)

const KAFKA_TOPIC = "wait-queue-events" // 대기열 0순위 유저 변경을 전체 api-server에 알림

var (
	writer *kafka.Writer
	reader *kafka.Reader
)

func InitMQClient() error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	writer = &kafka.Writer{
		Addr:     kafka.TCP("kafka-service:9092"),
		Topic:    KAFKA_TOPIC,
		Balancer: &kafka.LeastBytes{},
	}
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"kafka-service:9092"},
		Topic:    KAFKA_TOPIC,
		GroupID:  hostname, // 모든 서버가 다른 ID를 가져야 전원 수신 가능
		MinBytes: 10e3,     // 10KB
		MaxBytes: 10e6,     // 10MB
	})

	return nil
}

func CloseMQClient() {
	if err := writer.Close(); err != nil {
		slog.Error("Failed to close MQ writer", "error", err)
	}
	if err := reader.Close(); err != nil {
		slog.Error("Failed to close MQ reader", "error", err)
	}
}

// key: user_id:event_id, value: token
func buildKey(user_id int64, event_id string) string {
	return fmt.Sprintf("%d:%s", user_id, event_id)
}
func parseKey(key string) (user_id int64, event_id string, err error) {
	_, err = fmt.Sscanf(key, "%d:%s", &user_id, &event_id)
	if err != nil {
		// slog.Error("failed to parse key: %v", err)
		return
	}
	return user_id, event_id, nil
}

// Producer
func Write(ctx context.Context, key string, val string) error {
	return writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(val),
		},
	)
}
func BroadcastToken(ctx context.Context, user_id int64, event_id, token string) error {
	return Write(ctx, buildKey(user_id, event_id), token)
}

// Consumer
func RunConsumer(ctx context.Context, sseHub *sse.Hub) {
	for {
		select {
		case <-ctx.Done():
			slog.Error("RunConsumer done")
			return

		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("error while reading message: %v", err)
				break
			}

			// 다른 api-server에서 브로드캐스트된 토큰을 SSE Hub로 전달
			// 유저가 토큰을 전달받고 SSE 연결을 끊음
			user_id, event_id, err := parseKey(string(msg.Key))
			if err != nil {
				slog.Error("failed to parse key", "error", err, "key", msg.Key)
				continue
			}
			token := string(msg.Value)
			if token == "" {
				slog.Error("empty token received for key", "key", msg.Key)
				continue
			}

			if !sseHub.SendToken(ctx, user_id, event_id, token) {
				slog.Info("broadcasted user not exist in the sse hub", "user_id", user_id, "event_id", event_id)
			}
		}
	}
}
