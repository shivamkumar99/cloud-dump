# ── Stage 1: Build cloud-dump binary ─────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Download deps first (layer cache)
COPY go.mod go.sum ./
RUN go mod download

# Build the binary
COPY . .
RUN go build -o cloud-dump .

# ── Stage 2: PostgreSQL 17 with cloud-dump installed ─────────────────────────
FROM postgres:17-alpine

# ca-certificates needed for TLS connections to Storj
RUN apk add --no-cache ca-certificates

# Install cloud-dump binary
COPY --from=builder /app/cloud-dump /usr/local/bin/cloud-dump

# Install WAL wrapper scripts (archive + restore)
COPY docker/wal-archive.sh /usr/local/bin/wal-archive
COPY docker/wal-restore.sh /usr/local/bin/wal-restore
RUN chmod +x /usr/local/bin/wal-archive /usr/local/bin/wal-restore
