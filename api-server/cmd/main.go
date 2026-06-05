package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gonflix/no24/api-server/internal/mq"
	"github.com/gonflix/no24/api-server/internal/queue"
	"github.com/gonflix/no24/api-server/internal/service"
	svc "github.com/gonflix/no24/api-server/internal/service"
	"github.com/gonflix/no24/api-server/internal/sse"
	"github.com/gonflix/no24/api-server/internal/types"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

func init() {
	godotenv.Load("../.env") // api-server/cmd/ 에서 실행 시
	godotenv.Load(".env")    // api-server/ 에서 실행 시 (이미 설정된 값은 덮어쓰지 않음)

	//
	// 1. Handler 옵션 설정
	opts := &slog.HandlerOptions{
		AddSource: true, // 이 옵션이 caller(파일 및 라인) 정보를 추가합니다.
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// 로그의 소스(caller) 정보인 경우 처리
			if a.Key == slog.SourceKey {
				source, ok := a.Value.Any().(*slog.Source)
				if !ok {
					return a
				}
				// 전체 경로에서 마지막 파일명만 추출 (예: /app/main.go -> main.go)
				shortFile := filepath.Base(source.File)

				// 파일명:라인번호 형식의 문자열로 변경하거나 다시 구조화
				return slog.String("caller", fmt.Sprintf("%s:%d", shortFile, source.Line))
			}
			return a
		},
	}

	// 2. JSON 포맷이나 Text 포맷 핸들러 생성
	// 실무(Kubernetes/Cloud)에서는 파싱이 쉬운 JSONHandler를 주로 사용합니다.
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))

	// 3. 전역 로거로 설정 (선택 사항)
	slog.SetDefault(logger)

	service.InitService(types.RunEnv(os.Getenv("RUN_ENV")), os.Getenv("TOKEN_ISSUER"), os.Getenv("TOKEN_AUDIENCE"), os.Getenv("TOKEN_KEY_ID"))
}

func main() {
	mainCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// MQ Client
	if err := mq.InitMQClient(); err != nil {
		slog.Error("Failed to initialize MQ Client", "error", err)
		return
	}
	defer mq.CloseMQClient()

	// WaitingQueue Repository
	wqRepository := queue.NewWaitingQueueRepository(mainCtx)
	defer wqRepository.Close()

	// SSE Hub
	sseHub := sse.NewHub()

	// Echo Instance
	e := echo.New()
	e.Use(middleware.RequestLogger())
	e.Use(middleware.Recover())

	// Routes
	e.GET("/enter", func(c *echo.Context) error {
		return sseHub.HandleSSE(c, mainCtx, wqRepository)
	})
	e.GET("/.well-known/jwks.json", func(c *echo.Context) error {
		return service.HandleJWKS(c)
	})

	var mainWG sync.WaitGroup
	mainWG.Go(func() { // SSE Hub
		sseHub.Run(mainCtx)
	})
	mainWG.Go(func() { // WaitingQueue Worker
		svc.RunWaitingQueueWorkerAll(mainCtx, wqRepository, sseHub)
	})
	mainWG.Go(func() { // Kafka Consumer
		mq.RunConsumer(mainCtx, sseHub)
	})

	sc := echo.StartConfig{Address: ":8080"}
	if err := sc.Start(mainCtx, e); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error(err.Error())
	}

	mainWG.Wait()

	slog.Info("API Server Shutdown...")
}
