package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hzn0033/high-performance-api-gateway/internal/config"
	"github.com/hzn0033/high-performance-api-gateway/internal/gateway"
	"github.com/hzn0033/high-performance-api-gateway/internal/ratelimit"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	path := flag.String("config", "configs/local.json", "path to gateway config")
	flag.Parse()
	c, err := config.Load(*path)
	if err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cache := redis.NewClient(&redis.Options{Addr: c.RedisAddr, Password: os.Getenv("REDIS_PASSWORD"), DialTimeout: 100 * time.Millisecond, ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer cache.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	if err := cache.Ping(ctx).Err(); err != nil {
		slog.Warn("Redis unavailable; requests will bypass cache on errors", "error", err)
	}
	cancel()
	limiter := ratelimit.New(c.Rate, c.Burst, c.MaxClients)
	stats := &gateway.Stats{}
	server := &http.Server{Addr: c.HTTPAddr, Handler: gateway.NewHTTP(c, cache, limiter, stats), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: time.Duration(c.TimeoutSeconds) * time.Second, WriteTimeout: time.Duration(c.TimeoutSeconds+1) * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	grpcServer, closeConnections, err := gateway.NewGRPC(c.GRPCRoutes, limiter, stats, c.MaxConcurrent, time.Duration(c.TimeoutSeconds)*time.Second)
	if err != nil {
		return err
	}
	defer closeConnections()
	httpListener, err := net.Listen("tcp", c.HTTPAddr)
	if err != nil {
		return err
	}
	defer httpListener.Close()
	grpcListener, err := net.Listen("tcp", c.GRPCAddr)
	if err != nil {
		return err
	}
	defer grpcListener.Close()
	failures := make(chan error, 2)
	go func() { failures <- server.Serve(httpListener) }()
	go func() { failures <- grpcServer.Serve(grpcListener) }()
	slog.Info("gateway listening", "http", c.HTTPAddr, "grpc", c.GRPCAddr)
	stop, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-stop.Done():
	case err = <-failures:
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}
	}
	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	grpcDone := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(grpcDone) }()
	if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
		server.Close()
	}
	select {
	case <-grpcDone:
	case <-shutdown.Done():
		grpcServer.Stop()
		<-grpcDone
	}
	return err
}
