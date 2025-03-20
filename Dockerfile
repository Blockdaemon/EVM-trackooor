# Build stage
FROM golang:1.24.0-alpine3.21 AS builder

# Install git and build dependencies
RUN apk add --no-cache git build-base

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

ENV GOPRIVATE=gitlab.com/blockdaemon,go.blockdaemon.com/blockdaemon,gitlab.com/Blockdaemon,go.blockdaemon.com

# Create .netrc file with Gitlab credentials in order to access private repos. Had to split it up due to a docker compose regression on secrets
RUN --mount=type=secret,id=gitlab_username echo -n "machine gitlab.com login $(cat /run/secrets/gitlab_username)" > ~/.netrc
RUN --mount=type=secret,id=gitlab_token echo " password $(cat /run/secrets/gitlab_token)" >> ~/.netrc


# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o evm-trackooor

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates curl

WORKDIR /app

# Create data directory
RUN mkdir -p /app/data

# Copy the binary from builder
COPY --from=builder /app/evm-trackooor .

# Copy config and data files
# COPY config.json .
# COPY data/ /app/data/

# Expose health check port (default to 8080, can be overridden)
EXPOSE 8080

# Set the binary as the entrypoint
ENTRYPOINT ["./evm-trackooor"]

# Default command (can be overridden)
CMD ["track", "realtime", "--config", "./config.json", "--verbose", "--health-port", "8080"]
