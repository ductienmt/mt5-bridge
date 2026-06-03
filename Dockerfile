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
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o sender    ./cmd/sender
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o relay     ./cmd/relay
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o api       ./cmd/api

# Tiny runtime image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Create non-root user
RUN adduser -D -g '' appuser

COPY --from=builder /build/http-bridge .
COPY --from=builder /build/tcp-bridge .
COPY --from=builder /build/sender .
COPY --from=builder /build/relay .
COPY --from=builder /build/api .

# Entrypoint: chạy binary được chỉ định qua CMD
COPY <<-'EOF' /app/entrypoint.sh
#!/bin/sh
echo "=============================================="
echo " MT5 Trading Bridge"
echo "=============================================="
exec /app/"${BINARY:-http-bridge}"
EOF

RUN chmod +x /app/entrypoint.sh

USER appuser

EXPOSE 8080 8081 8082 8083

ENTRYPOINT ["/app/entrypoint.sh"]
