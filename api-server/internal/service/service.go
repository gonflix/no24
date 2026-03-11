package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/gonflix/no24/api-server/internal/queue"
	"github.com/gonflix/no24/api-server/internal/sse"
)

const TOKEN_DURATION = 10 * time.Minute

const mySigningKey = "71c150277be553d6c584b7a7bc403bfa9a01935c49a0cb8e872516f712086291"

func createJWT(user_id int64, event_id string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"user_id":  user_id,
		"event_id": event_id,
		"exp":      time.Now().Add(TOKEN_DURATION).Unix(),
	})
	return token.SignedString(mySigningKey)
}

func RunWaitingQueueWorkerAll(ctx context.Context, wqRepository *queue.WaitingQueueRepository, sseHub *sse.Hub) {
	var wg sync.WaitGroup

	wqRepository.Snapshots.Range(func(event_id string, snapshot *queue.WaitingQueueSnapshot) bool {
		wg.Go(func() {
			waitingQueueWorker(ctx, event_id, wqRepository, sseHub)
		})
		return true
	})

	wg.Wait()
}

func waitingQueueWorker(ctx context.Context, event_id string, wqRepository *queue.WaitingQueueRepository, sseHub *sse.Hub) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	slog.Info("waitingQueueWorker start", "event_id", event_id)

	for {
		select {
		case <-ctx.Done():
			slog.Error("waitingQueueWorker done", "event_id", event_id)
			return

		case <-ticker.C:
			// 1분마다 스냅샷 업데이트
			err := wqRepository.UpdateSnapshot(ctx, event_id)
			if err != nil {
				slog.Error("error update snapshot", "error", err, "event_id", event_id)
			}

		default:
			// 0순위 유저를 제거하고 토큰 발급
			slog.Info("waitingQueueWorker trying pop!", "event_id", event_id)
			user_id, err := wqRepository.Pop(ctx, event_id)
			if err != nil {
				if err == queue.ErrQueueEmpty {
					time.Sleep(1 * time.Second)
					continue
				}

				slog.Error("error pop from waiting room", "error", err, "event_id", event_id)
				continue
			}
			token, err := createJWT(user_id, event_id)
			if err != nil {
				slog.Error("error creating JWT", "error", err, "event_id", event_id, "user_id", user_id)
				continue
			}

			slog.Info("waitingQueueWorker pop user", "event_id", event_id, "user_id", user_id, "token", token)

			// SSE로 토큰 전달
			sseHub.SendToken(ctx, user_id, event_id, token)
			//

			// 그 결과 브라우저는 SSE 종료, 토큰들고 java  예매페이지로 리다이렉트
		}
	}
}
