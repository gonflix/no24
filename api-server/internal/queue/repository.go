package queue

import (
	"context"
	"errors"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

type WaitingQueueRepository struct {
	client *redis.Client
	Events []string
}

var ErrQueueEmpty = errors.New("waiting room is empty")

func NewWaitingQueueRepository(ctx context.Context) *WaitingQueueRepository {
	client := redis.NewClient(&redis.Options{
		Addr: "redis-service:6379",
	})
	if err := client.Ping(ctx).Err(); err != nil {
		slog.Error("Redis 연결 실패", "error", err)
		panic(err)
	}
	slog.Info("Redis connected", "addr", "redis-service:6379")

	events, err := ReloadEvents(ctx)
	if err != nil {
		slog.Error("ReloadEvents failed", "error", err)
		panic(err)
	}

	names := make([]string, 0, len(events))
	for _, e := range events {
		names = append(names, e.Name)
	}

	return &WaitingQueueRepository{
		client: client,
		Events: names,
	}
}

func (r *WaitingQueueRepository) Close() { r.client.Close() }

// Add enqueues the user and returns their monotonic ticket number N.
// Position in queue = N - 1 - served (0 means it's their turn).
func (r *WaitingQueueRepository) Add(ctx context.Context, event_id string, user_id int64) (ticketN int64, err error) {
	ticketN, err = r.client.Incr(ctx, totalKey(event_id)).Result()
	if err != nil {
		slog.Error("redis incr total failed", "event_id", event_id, "user_id", user_id, "error", err)
		return -1, err
	}
	slog.Info("user enqueued", "event_id", event_id, "user_id", user_id, "ticketN", ticketN)
	return ticketN, nil
}

// Pop advances the served counter by 1 if there are users waiting.
// Returns ErrQueueEmpty when served >= total.
func (r *WaitingQueueRepository) Pop(ctx context.Context, event_id string) error {
	total, err := r.client.Get(ctx, totalKey(event_id)).Int64()
	if err == redis.Nil {
		return ErrQueueEmpty
	}
	if err != nil {
		return err
	}

	served, err := r.client.Get(ctx, servedKey(event_id)).Int64()
	if err != nil && err != redis.Nil {
		return err
	}

	if served >= total {
		return ErrQueueEmpty
	}

	return r.client.Incr(ctx, servedKey(event_id)).Err()
}

func (r *WaitingQueueRepository) GetServedCount(ctx context.Context, event_id string) (int64, error) {
	val, err := r.client.Get(ctx, servedKey(event_id)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// Redis Counter Keys
//
// totalKey counts how many users have joined the queue (monotonic).
func totalKey(event_id string) string { return "queue:total:" + event_id }

// servedKey counts how many users have been served (advanced by Pop).
func servedKey(event_id string) string { return "queue:served:" + event_id }
