# Multi-stage build for FlashDB
FROM golang:1.22.2-alpine AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o flashdb ./src

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS if needed
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/flashdb .

# Create data directory for persistence
RUN mkdir -p /app/data

# Expose default port (adjust as needed for your API)
EXPOSE 8080

# Health check (adjust as needed)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD test -f /app/data/MANIFEST || exit 1

# Run the application
ENTRYPOINT ["./flashdb"]
