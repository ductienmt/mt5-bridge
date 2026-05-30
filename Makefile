.PHONY: all build build-cli run-http run-tcp run-signal-cli run-sender up down clean test lint fmt

# Default target
all: build

# ─── Build ───────────────────────────────────────────────────────────────
build:
	@echo "Building all binaries..."
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/http-bridge    ./cmd/http
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/tcp-bridge    ./cmd/tcp
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/sender        ./cmd/sender
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/relay        ./cmd/relay
	@echo "Done → bin/"

build-cli:
	docker build -f Dockerfile.cli -t mt5-signal-cli .
	@echo "Done → mt5-signal-cli image"

# ─── Run locally ─────────────────────────────────────────────────────────
run-http:
	go run ./cmd/http

run-tcp:
	go run ./cmd/tcp

run-sender:
	go run ./cmd/sender -action OPEN -side BUY -symbol EURUSD -lot 0.1 -sl 1.0800 -tp 1.0900

# Interactive TCP CLI
run-signal-cli:
	go run ./cmd/signal-cli

up: build-image
	docker compose up -d
	@docker compose ps

# Up + interactive CLI
up-cli: build-image
	docker compose up -d signal-cli

down:
	docker compose down

logs:
	docker compose logs -f

logs-http:
	docker compose logs -f http-bridge

logs-tcp:
	docker compose logs -f tcp-bridge

restart: down up

# Signal CLI shortcuts ────────────────────────────────────────────────
# Interactive mode (persistent connection, persistent defaults)
cli:
	go run ./cmd/signal-cli

# Docker interactive
docker-cli:
	docker compose run --rm signal-cli

# OPEN BUY
cli-buy:
	go run ./cmd/signal-cli -action OPEN -side BUY -symbol EURUSD -lot 0.1 -sl 1.0800 -tp 1.0900

# OPEN SELL
cli-sell:
	go run ./cmd/signal-cli -action OPEN -side SELL -symbol EURUSD -lot 0.1 -sl 1.0900 -tp 1.0800

# CLOSE symbol
cli-close:
	go run ./cmd/signal-cli -action CLOSE -symbol EURUSD

# EDIT SL/TP
cli-edit:
	go run ./cmd/signal-cli -action EDIT -symbol EURUSD -sl 1.0750 -tp 1.0950

# Pipe mode: send JSON from file or stdin
cli-pipe:
	go run ./cmd/signal-cli -connect - < signal.json

# ─── Legacy sender shortcuts (still work, reads bridge via sender.go) ────
buy-http:
	go run ./cmd/sender -action OPEN -side BUY -symbol EURUSD -lot 0.1 -sl 1.0800 -tp 1.0900 -host localhost:8080

buy-tcp:
	go run ./cmd/sender -action OPEN -side BUY -symbol EURUSD -lot 0.1 -sl 1.0800 -tp 1.0900 -host localhost:8081 -tcp

sell-tcp:
	go run ./cmd/sender -action OPEN -side SELL -symbol EURUSD -lot 0.1 -sl 1.0900 -tp 1.0800 -host localhost:8081 -tcp

close-all-tcp:
	go run ./cmd/sender -action CLOSE -host localhost:8081 -tcp

close-http:
	go run ./cmd/sender -action CLOSE -symbol EURUSD -host localhost:8080

# ─── Dev ───────────────────────────────────────────────────────────────
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
