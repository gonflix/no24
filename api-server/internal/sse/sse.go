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

func HandleSSE(
	c *echo.Context,
	ctx context.Context,
	wqRepository *queue.WaitingQueueRepository,
	jwtCreator func(userID int64, eventID string) (string, error),
) error {
	eventID := c.Request().URL.Query().Get("event_id")
	userIDs := c.Request().URL.Query().Get("user_id")
	userID, err := strconv.ParseInt(userIDs, 10, 64)
	if err != nil {
		slog.Error("sse handler error", "error", err, "user_id", userIDs)
		return err
	}

	slog.Info("sseHandler start", "user_id", userID, "event_id", eventID)

	// Add user to the waiting queue and get their ticket number.
	ticketN, err := wqRepository.Add(c.Request().Context(), eventID, userID)
	if err != nil {
		slog.Error("sse add error", "error", err, "user_id", userID)
		return err
	}

	c.Response().Header().Set("Content-Type", "text/event-stream")
	c.Response().Header().Set("Cache-Control", "no-cache")
	c.Response().Header().Set("Connection", "keep-alive")
	c.Response().Header().Set("Access-Control-Allow-Origin", "*")

	rc := http.NewResponseController(c.Response())

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("sseHandler closed", "user_id", userID)
			return nil

		case <-c.Request().Context().Done():
			slog.Info("sse client disconnected", "user_id", userID)
			return nil

		case <-ticker.C:
			// Get how many users have been served so far.
			served, err := wqRepository.GetServedCount(c.Request().Context(), eventID)
			if err != nil {
				slog.Error("sse get served count error", "error", err, "user_id", userID)
				continue
			}

			// ahead is how many users are ahead in the queue.
			ahead := ticketN - 1 - served
			if ahead <= 0 { // It's the user's turn. Issue JWT and close connection.
				token, err := jwtCreator(userID, eventID)
				if err != nil {
					slog.Error("sse jwt create error", "error", err, "user_id", userID)
					continue
				}
				if err := writeResp(c.Response(), userID, 0, token); err != nil {
					slog.Error("sse write token error", "error", err, "user_id", userID)
					continue
				}
				rc.Flush()
				slog.Info("sse jwt issued and closed", "user_id", userID)
				return nil
			}

			// Not user's turn yet. Send update with how many are ahead.
			if err := writeResp(c.Response(), userID, ahead, ""); err != nil {
				slog.Error("sse write resp error", "error", err, "user_id", userID)
				continue
			}
			if err := rc.Flush(); err != nil {
				slog.Error("sse flush error", "error", err, "user_id", userID)
				continue
			}
			slog.Info("sse write resp", "user_id", userID, "ahead", ahead)
		}
	}
}

func writeResp(w io.Writer, userID int64, ahead int64, token string) error {
	rs := model.EnterResponse{
		Sequence: ahead,
		Token:    token,
	}
	payload, err := json.Marshal(rs)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		slog.Error("sse write error", "error", err, "user_id", userID)
		return err
	}

	return nil
}
