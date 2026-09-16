.PHONY: test bench build run demo up down

test:
	go test -race -count=1 ./...
	go vet ./...

bench:
	go test ./internal/ratelimit ./internal/gateway -run '^$$' -bench . -benchmem

build:
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/demo ./cmd/demo

run:
	go run ./cmd/gateway -config configs/local.json

demo:
	go run ./cmd/demo

up:
	docker compose up --build -d

down:
	docker compose down
