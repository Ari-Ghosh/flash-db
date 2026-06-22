# Multi-stage build for flashDB
FROM golang:1.25.11-alpine AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the CLI binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o flashdb ./cmd/flashdb

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /build/flashdb .

RUN mkdir -p /app/data

# Default ports: metrics, Raft
EXPOSE 9090 6000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["./flashdb", "status", "/app/data"]

# Default: start the server
ENTRYPOINT ["./flashdb"]
CMD ["serve", "--dir", "/app/data", "--metrics-addr", ":9090"]
