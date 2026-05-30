.PHONY: all build run-http run-tcp run-sender up down clean test lint fmt

# Default target
all: build

# ─── Build ───────────────────────────────────────────────────────────────
build:
	@echo "Building all binaries..."
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/http-bridge ./cmd/http
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/tcp-bridge ./cmd/tcp
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/sender    ./cmd/sender
	@echo "Done → bin/"

# ─── Run locally ─────────────────────────────────────────────────────────
run-http:
	go run ./cmd/http

run-tcp:
	go run ./cmd/tcp

run-sender:
	go run ./cmd/sender --action BUY --symbol EURUSD --lot 0.1 --sl 1.0800 --tp 1.0900

# ─── Docker ─────────────────────────────────────────────────────────────
build-image:
	docker build -t mt5-bridge:latest .

up: build-image
	docker compose up -d
	@docker compose ps

down:
	docker compose down

logs:
	docker compose logs -f

logs-http:
	docker compose logs -f http-bridge

logs-tcp:
	docker compose logs -f tcp-bridge

restart: down up

# ─── Sender helpers ─────────────────────────────────────────────────────
# Gửi BUY bằng HTTP
buy-http:
	go run ./cmd/sender -action BUY -symbol EURUSD -lot 0.1 -sl 1.0800 -tp 1.0900 -host localhost:8080

# Gửi BUY bằng TCP
buy-tcp:
	go run ./cmd/sender -host localhost:8081 -action BUY -symbol EURUSD -lot 0.1 -sl 1.0800 -tp 1.0900

# Close all
close-all-http:
	go run ./cmd/sender -action CLOSE_ALL -symbol EURUSD -host localhost:8080

close-all-tcp:
	go run ./cmd/sender -action CLOSE_ALL -symbol EURUSD -host localhost:8081

# ─── Dev ────────────────────────────────────────────────────────────────
test:
	go test ./...

fmt:
	go fmt ./...

lint:
	golangci-lint run ./... || true

# ─── Cleanup ────────────────────────────────────────────────────────────
clean:
	rm -rf bin/
	docker compose down --rmi local 2>/dev/null || true
