package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/job"
)

const (
	defaultHTTPAddress     = ":8080"
	databaseStartupTimeout = 5 * time.Second
	readinessTimeout       = time.Second
	shutdownTimeout        = 5 * time.Second
)

type readinessCheck func(context.Context) error

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(
		ctx,
		os.Getenv("MILL_HTTP_ADDR"),
		os.Getenv("MILL_DATABASE_URL"),
		os.Getenv("MILL_OUTPUT_ROOT_URI"),
		os.Getenv("MILL_PARALLELISM"),
	); err != nil {
		log.Fatalf("run Mill: %v", err)
	}
}

func run(ctx context.Context, address, databaseURL, outputRootURI, parallelismValue string) error {
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	if address == "" {
		address = defaultHTTPAddress
	}
	if databaseURL == "" {
		return errors.New("MILL_DATABASE_URL is required")
	}
	if outputRootURI == "" {
		return errors.New("MILL_OUTPUT_ROOT_URI is required")
	}
	parallelism, err := parseParallelism(parallelismValue)
	if err != nil {
		return err
	}

	database, err := openDatabase(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()

	jobRepository, err := job.NewRepository(database, outputRootURI)
	if err != nil {
		return err
	}
	jobService, err := job.NewService(jobRepository, job.JSONLPartitioner{}, parallelism)
	if err != nil {
		return err
	}
	jobHandler := job.NewHandler(jobService, log.Default())
	executionLoop, err := configureExecution(ctx, databaseURL, jobRepository)
	if err != nil {
		return err
	}
	if executionLoop != nil {
		defer executionLoop.close()
	}

	server := &http.Server{
		Addr:              address,
		Handler:           newHandler(database.Ping, jobHandler),
		ReadHeaderTimeout: 5 * time.Second,
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()
	executionErrors := make(chan error, 1)
	if executionLoop != nil {
		executionDone := make(chan struct{})
		go func() {
			defer close(executionDone)
			executionErrors <- executionLoop.run(ctx)
		}()
		defer func() {
			cancelRun()
			<-executionDone
		}()
	}
	defer server.Close()

	log.Printf("Mill HTTP server listening on %s", listener.Addr())

	select {
	case err := <-executionErrors:
		if ctx.Err() == nil {
			return fmt.Errorf("execution coordinator stopped: %w", err)
		}
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	if err := <-serveErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	return nil
}

func parseParallelism(value string) (int, error) {
	if value == "" {
		return 0, errors.New("MILL_PARALLELISM is required")
	}
	parallelism, err := strconv.Atoi(value)
	if err != nil || parallelism < 1 || parallelism > 10000 {
		return 0, errors.New("MILL_PARALLELISM must be an integer between 1 and 10000")
	}
	return parallelism, nil
}

func openDatabase(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse MILL_DATABASE_URL: %w", err)
	}

	database, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL connection pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, databaseStartupTimeout)
	defer cancel()
	if err := database.Ping(pingCtx); err != nil {
		database.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}

	return database, nil
}

func newHandler(checkReady readinessCheck, jobHandler *job.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleLiveness)
	mux.HandleFunc("GET /livez", handleLiveness)
	mux.HandleFunc("GET /readyz", handleReadiness(checkReady))
	if jobHandler != nil {
		jobHandler.RegisterRoutes(mux)
	}
	return mux
}

func handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writeStatus(w, http.StatusOK, "ok")
}

func handleReadiness(check readinessCheck) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()

		if err := check(ctx); err != nil {
			writeStatus(w, http.StatusServiceUnavailable, "not_ready")
			return
		}

		writeStatus(w, http.StatusOK, "ready")
	}
}

func writeStatus(w http.ResponseWriter, statusCode int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = io.WriteString(w, fmt.Sprintf("{\"status\":%q}\n", status))
}
