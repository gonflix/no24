package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gonflix/no24/api-server/internal/model"
	"github.com/gonflix/no24/api-server/internal/queue"
	"github.com/labstack/echo/v5"
)

type hubChannel struct {
	hubKey
	ch chan string
}

type hubKey struct {
	user_id  int64
	event_id string
}

type Hub struct {
	channels   map[hubKey]chan string
	register   chan hubChannel
	unregister chan hubKey
	// broadcast  chan string // Redis에서 가져온 최신 랭킹 데이터
}

func NewHub() *Hub {
	return &Hub{
		channels:   make(map[hubKey]chan string),
		register:   make(chan hubChannel),
		unregister: make(chan hubKey),
	}
}

func (h *Hub) SendToken(ctx context.Context, user_id int64, event_id string, token string) {
	key := hubKey{user_id: user_id, event_id: event_id}

	client, ok := h.channels[key]
	if !ok {
		slog.Error("sse client not found", "user_id", user_id, "event_id", event_id)
		return
	}
	client <- token
}

func (h *Hub) Run(ctx context.Context) {
	slog.Info("sse hub run")

	for {
		select {
		case <-ctx.Done():
			slog.Info("sse hub done")
			return
		case info := <-h.register: // 채널 등록
			key := hubKey{user_id: info.user_id, event_id: info.event_id}
			h.channels[key] = info.ch

		case key := <-h.unregister: // 채널 해제
			close(h.channels[key])
			delete(h.channels, key)

			// case message := <-h.broadcast:
			// 	// 모든 클라이언트에게 동시에 전송 (Non-blocking 권장)?
			// 	for client := range h.clients {
			// 		select {
			// 		case client <- message:
			// 		default: // 클라이언트가 느리면 무시 (Backpressure)?
			// 		}
			// 	}
		}
	}
}

func (h *Hub) HandleSSE(c *echo.Context, ctx context.Context, wqRepository *queue.WaitingQueueRepository) error {
	eventID := c.Request().URL.Query().Get("event_id")
	userIDs := c.Request().URL.Query().Get("user_id")
	userID, err := strconv.ParseInt(userIDs, 10, 64)
	if err != nil {
		slog.Error("sse handler error", "error", err, "user_id", userIDs)
		return err
	}

	slog.Debug("sseHandler start", "user_id", userID, "event_id", eventID)	

	// SSE 채널 등록
	hubkey := hubKey{user_id: userID, event_id: eventID}
	jwtChan := make(chan string)
	h.register <- hubChannel{hubKey: hubkey, ch: jwtChan}

	defer func() {
		h.unregister <- hubkey // SSE 채널 해제
	}()

	c.Response().Header().Set("Content-Type", "text/event-stream")
	c.Response().Header().Set("Cache-Control", "no-cache")
	c.Response().Header().Set("Connection", "keep-alive")

	// You may need this locally for CORS requests
	// w.Header().Set("Access-Control-Allow-Origin", "*")

	rc := http.NewResponseController(c.Response())

	_, err = wqRepository.Add(c.Request().Context(), eventID, userID)
	if err != nil {
		slog.Error("sse add error", "error", err, "user_id", userID)
		return err
	}

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("sseHandler closed", "user_id", userID)
			return nil

		case <-c.Request().Context().Done(): // 클라이언트 종료
			slog.Info("sse client disconnected", "user_id", userID)
			return nil

		case token := <-jwtChan: // 0순위 유저에게 JWT 토큰 발송
			err := writeResp(c.Response(), userID, 0, token)
			if err != nil {
				slog.Error("sse write enter resp error", "error", err, "user_id", userID)
				continue
			}
			if err := rc.Flush(); err != nil {
				slog.Error("sse flush error", "error", err, "user_id", userID)
				continue
			}
			slog.Debug("sse write token", "user_id", userID, "token", token)

		case <-ticker.C: // 주기적으로 순서 알림
			rank, err := wqRepository.Get(c.Request().Context(), eventID, userID)
			if err != nil {
				slog.Error("sse get rank error", "error", err, "user_id", userID)
				continue
			}
			err = writeResp(c.Response(), userID, rank, "")
			if err != nil {
				slog.Error("sse write enter resp error", "error", err, "user_id", userID)
				continue
			}
			if err := rc.Flush(); err != nil {
				slog.Error("sse flush error", "error", err, "user_id", userID)
				continue
			}
			slog.Debug("sse write resp", "user_id", userID, "rank", rank)
		}
	}
}

func writeResp(w io.Writer, userID int64, rank int64, token string) error {
	rs := model.EnterResponse{
		Sequence: rank + 1,
		Token:    token,
	}
	payload, err := json.Marshal(rs)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil { // sse 규격?
		slog.Error("sse write error", "error", err, "user_id", userID)
		return err
	}

	return nil
}
