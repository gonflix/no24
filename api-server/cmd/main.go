package main

import "fmt"

func main() {
	fmt.Println("Hello from no24 API server!")
	// mux := http.NewServeMux()

	// mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
	// 	w.WriteHeader(http.StatusOK)
	// 	fmt.Fprintln(w, "OK")
	// })

	// mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	// 	w.WriteHeader(http.StatusOK)
	// 	fmt.Fprintln(w, "Hello from no24 API server!")
	// })

	// server := &http.Server{
	// 	Addr:         ":8080",
	// 	Handler:      mux,
	// 	ReadTimeout:  10 * time.Second,
	// 	WriteTimeout: 10 * time.Second,
	// 	IdleTimeout:  60 * time.Second,
	// }

	// 별도 고루틴에서 서버 시작
	// go func() {
	// 	log.Printf("Server listening on %s", server.Addr)
	// 	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
	// 		log.Fatalf("Server failed to start: %v", err)
	// 	}
	// }()

	// OS 시그널 수신 채널 (SIGINT, SIGTERM)
	// ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	// defer stop()

	// quit := make(chan os.Signal, 1)
	// signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// <-quit

	// 최대 30초 대기 후 강제 종료
	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()

	// if err := server.Shutdown(ctx); err != nil {
	// 	log.Fatalf("Server forced to shutdown: %v", err)
	// }

}
