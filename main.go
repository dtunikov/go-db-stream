package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/executor"
	"github.com/dtunikov/go-db-stream/pkg/logger"

	"go.uber.org/zap"
)

var (
	configPathFlag = flag.String("config", os.Getenv("CONFIG_PATH"), "Path to the config file e.g. config.yaml")
	help           = flag.Bool("help", false, "Prints the help message")
)

func serve() error {
	flag.Parse()

	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	cfg, err := config.LoadConfig(*configPathFlag)
	if err != nil {
		return fmt.Errorf("could not load config: %w", err)
	}

	logger, err := logger.NewLogger(cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("could not create logger: %w", err)
	}

	executor, err := executor.NewExecutor(cfg.Executor, logger)
	if err != nil {
		return fmt.Errorf("could not create executor: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	err = executor.Run(&wg)
	if err != nil {
		return fmt.Errorf("could not run executor: %w", err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello, world!"))
	})

	// Create a new HTTP server
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", cfg.Server.Port),
		// TODO: use handler later
		Handler: http.DefaultServeMux,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Starting server", zap.String("address", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Server error", zap.Error(err))
		}
	}()

	// Create a channel to listen for OS signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	// Wait for a termination signal
	go func() {
		sig := <-signalChan
		logger.Info("Received signal", zap.String("signal", sig.String()))
		cancel()
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	logger.Info("Shutting down server")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("Shutdown failed", zap.Error(err))
	}

	// maybe use context with timeout
	executor.Stop()
	wg.Wait()
	logger.Info("Server stopped")
	return nil
}

func main() {
	if err := serve(); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
