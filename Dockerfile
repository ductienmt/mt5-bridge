# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git

# Cache go modules
COPY go.mod go.sum* ./
RUN go mod download

# Copy source
COPY . .

# Build all binaries
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o http-bridge ./cmd/http
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o tcp-bridge ./cmd/tcp
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o sender ./cmd/sender

# Tiny runtime image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Create non-root user
RUN adduser -D -g '' appuser

COPY --from=builder /build/http-bridge .
COPY --from=builder /build/tcp-bridge .
COPY --from=builder /build/sender .

# Entrypoint wrapper that runs both servers + sender
COPY <<-'EOF' /app/entrypoint.sh
#!/bin/sh
set -e

echo "=============================================="
echo " MT5 Trading Bridge"
echo " HTTP Bridge : ${MT5_HTTP_PORT:-8080}"
echo " TCP  Bridge : ${MT5_TCP_PORT:-8081}"
echo "=============================================="

# Start both bridges in background
echo "[entrypoint] Starting HTTP bridge on :${MT5_HTTP_PORT:-8080}"
/app/http-bridge &
HTTP_PID=$!

echo "[entrypoint] Starting TCP bridge on :${MT5_TCP_PORT:-8081}"
/app/tcp-bridge &
TCP_PID=$!

# Trap SIGTERM / SIGINT to stop children cleanly
trap "kill -TERM $HTTP_PID $TCP_PID 2>/dev/null; exit 0" TERM INT

# Keep alive — wait for any child
wait $HTTP_PID $TCP_PID
EOF

RUN chmod +x /app/entrypoint.sh

USER appuser

EXPOSE 8080 8081

ENTRYPOINT ["/app/entrypoint.sh"]
