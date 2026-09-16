package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	delay := flag.Duration("delay", 5*time.Millisecond, "simulated HTTP service work")
	flag.Parse()
	var calls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		timer := time.NewTimer(*delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=30")
		json.NewEncoder(w).Encode(map[string]any{"id": r.PathValue("id"), "name": "Mechanical keyboard", "price_cents": 8900})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "{\"requests\":%d}\n", calls.Load()) })
	httpServer := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	grpcServer := grpc.NewServer()
	healthService := health.NewServer()
	healthService.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthService)
	listener, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatal(err)
	}
	errors := make(chan error, 2)
	go func() { errors <- grpcServer.Serve(listener) }()
	go func() { errors <- httpServer.ListenAndServe() }()
	log.Print("demo upstream: HTTP :8081, gRPC :9091")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case err := <-errors:
		log.Print(err)
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	httpServer.Shutdown(shutdown)
	grpcServer.Stop()
}
