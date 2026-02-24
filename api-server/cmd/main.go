package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gonflix/no24/api-server/internal/queue"
	svc "github.com/gonflix/no24/api-server/internal/service"
	"github.com/gonflix/no24/api-server/internal/sse"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

func main() {

	// WaitingQueue Repository
	wqRepository := queue.NewWaitingQueueRepository()
	defer wqRepository.Close()

	// SSE Hub
	sseHub := sse.NewHub()

	// Echo Instance
	e := echo.New()
	e.Use(middleware.RequestLogger())
	e.Use(middleware.Recover())

	mainCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Routes
	e.GET("/enter", func(c *echo.Context) error {
		return sseHub.HandleSSE(c, mainCtx, wqRepository)
	})

	var mainWG sync.WaitGroup
	mainWG.Go(func() { // SSE Hub
		sseHub.Run(mainCtx)
	})
	mainWG.Go(func() { // WaitingQueue Worker
		svc.RunWaitingQueueWorkerAll(mainCtx, wqRepository, sseHub)
	})

	sc := echo.StartConfig{Address: ":8080"}
	if err := sc.Start(mainCtx, e); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error(err.Error())
	}

	mainWG.Wait()

	slog.Info("main done")
}
