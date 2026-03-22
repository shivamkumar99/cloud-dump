.PHONY: build vet fmt \
        test test-unit test-integration \
        test-wal test-backup test-restore \
        test-wal-unit \
        test-pg17 test-pg18 test-all \
        test-storj \
        docker-up docker-down docker-reset docker-wait \
        restore17-reset restore17-start restore17-wait \
        restore18-reset restore18-start restore18-wait \
        wal-build wal-up wal-down wal-wait \
        wal-restore-reset wal-restore-start wal-restore-wait \
        ci

DOCKER_COMPOSE := docker compose -f docker/docker-compose.yml

# ── Build ─────────────────────────────────────────────────────────────────────

build:
	go build -o cloud-dump .

vet:
	go vet ./...

fmt:
	gofmt -l -w .

# ── Unit tests ────────────────────────────────────────────────────────────────
# No external services required.

test-unit:
	go test -v -race ./internal/...

# Run only WAL unit tests (no Docker)
test-wal-unit:
	go test -v -race ./internal/pgbackup/... -run TestWal

# ── Docker helpers ────────────────────────────────────────────────────────────

docker-up:
	$(DOCKER_COMPOSE) up -d postgres17 postgres18 postgres17-restore postgres18-restore postgres17-wal pgadmin
	@$(MAKE) docker-wait

# Wait for all source instances to be ready.
docker-wait:
	@echo "Waiting for postgres17 (localhost:5432)..."
	@for i in $$(seq 1 30); do \
		$(DOCKER_COMPOSE) exec -T postgres17 pg_isready -U postgres -q && break; \
		echo "  attempt $$i/30 — postgres17 not ready"; \
		sleep 3; \
	done
	@$(DOCKER_COMPOSE) exec -T postgres17 pg_isready -U postgres -q \
		|| (echo "postgres17 did not become ready in time"; exit 1)
	@echo "postgres17 ready."
	@echo "Waiting for postgres18 (localhost:5434)..."
	@for i in $$(seq 1 30); do \
		$(DOCKER_COMPOSE) exec -T postgres18 pg_isready -U postgres -q && break; \
		echo "  attempt $$i/30 — postgres18 not ready"; \
		sleep 3; \
	done
	@$(DOCKER_COMPOSE) exec -T postgres18 pg_isready -U postgres -q \
		|| (echo "postgres18 did not become ready in time"; exit 1)
	@echo "postgres18 ready."
	@echo "Waiting for postgres17-wal (localhost:5436)..."
	@for i in $$(seq 1 30); do \
		$(DOCKER_COMPOSE) exec -T postgres17-wal pg_isready -U postgres -q && break; \
		echo "  attempt $$i/30 — postgres17-wal not ready"; \
		sleep 3; \
	done
	@$(DOCKER_COMPOSE) exec -T postgres17-wal pg_isready -U postgres -q \
		|| (echo "postgres17-wal did not become ready in time"; exit 1)
	@echo "postgres17-wal ready."

docker-down:
	$(DOCKER_COMPOSE) down --remove-orphans

# Remove containers AND volumes (resets all sample data).
docker-reset:
	$(DOCKER_COMPOSE) down -v

# ── Restore-target helpers: PG17 ─────────────────────────────────────────────

restore17-reset:
	@echo "Stopping postgres17-restore..."
	$(DOCKER_COMPOSE) stop postgres17-restore
	@echo "Wiping docker/restore-data/pg17/ ..."
	docker run --rm \
	  -v "$(CURDIR)/docker/restore-data/pg17:/data" \
	  alpine:latest \
	  sh -c "rm -rf /data/* /data/.[!.]* 2>/dev/null; true"
	@echo "postgres17-restore is empty. Run: cloud-dump restore --pgdata docker/restore-data/pg17 ..."
	@echo "Then:  make restore17-start"

restore17-start:
	$(DOCKER_COMPOSE) up -d postgres17-restore
	@$(MAKE) restore17-wait

restore17-wait:
	@echo "Waiting for postgres17-restore (localhost:5433)..."
	@for i in $$(seq 1 30); do \
		$(DOCKER_COMPOSE) exec -T postgres17-restore pg_isready -U postgres -q && break; \
		echo "  attempt $$i/30 — not ready"; \
		sleep 3; \
	done
	@$(DOCKER_COMPOSE) exec -T postgres17-restore pg_isready -U postgres -q \
		|| (echo "postgres17-restore did not become ready in time"; exit 1)
	@echo "postgres17-restore ready on localhost:5433"

# ── Restore-target helpers: PG18 ─────────────────────────────────────────────

restore18-reset:
	@echo "Stopping postgres18-restore..."
	$(DOCKER_COMPOSE) stop postgres18-restore
	@echo "Wiping docker/restore-data/pg18/ ..."
	docker run --rm \
	  -v "$(CURDIR)/docker/restore-data/pg18:/data" \
	  alpine:latest \
	  sh -c "rm -rf /data/* /data/.[!.]* 2>/dev/null; true"
	@echo "postgres18-restore is empty. Run: cloud-dump restore --pgdata docker/restore-data/pg18 ..."
	@echo "Then:  make restore18-start"

restore18-start:
	$(DOCKER_COMPOSE) up -d postgres18-restore
	@$(MAKE) restore18-wait

restore18-wait:
	@echo "Waiting for postgres18-restore (localhost:5435)..."
	@for i in $$(seq 1 30); do \
		$(DOCKER_COMPOSE) exec -T postgres18-restore pg_isready -U postgres -q && break; \
		echo "  attempt $$i/30 — not ready"; \
		sleep 3; \
	done
	@$(DOCKER_COMPOSE) exec -T postgres18-restore pg_isready -U postgres -q \
		|| (echo "postgres18-restore did not become ready in time"; exit 1)
	@echo "postgres18-restore ready on localhost:5435"

# ── Integration tests ─────────────────────────────────────────────────────────
# Requires: make docker-up
#
# PGURL17 / PGURL18 can be overridden:
#   PGURL17="postgres://repl_user:repl_password@myhost:5432/postgres?replication=yes" make test-pg17

PGURL17 ?= postgres://repl_user:repl_password@localhost:5432/postgres?replication=yes
PGURL18 ?= postgres://repl_user:repl_password@localhost:5434/postgres?replication=yes

# Default PGURL used by test-integration, test-backup, test-restore, test-wal
PGURL ?= $(PGURL17)

# Run all integration tests against the default source (PG17)
test-integration:
	PGURL="$(PGURL)" go test -tags integration -v -race -timeout 10m ./tests/integration/...

# Run only backup integration tests
test-backup:
	PGURL="$(PGURL)" go test -tags integration -v -race -timeout 10m ./tests/integration/... -run TestBackup

# Run only restore integration tests
test-restore:
	PGURL="$(PGURL)" go test -tags integration -v -race -timeout 10m ./tests/integration/... -run TestRestore

# Run only WAL integration tests
test-wal:
	PGURL="$(PGURL)" go test -tags integration -v -race -timeout 10m ./tests/integration/... -run TestWal

# Run integration tests against PG17 (default PGDATA, port 5432)
test-pg17:
	PGURL="$(PGURL17)" go test -tags integration -v -race -timeout 10m ./tests/integration/...

# Run integration tests against PG18 (custom PGDATA, port 5434)
test-pg18:
	PGURL="$(PGURL18)" go test -tags integration -v -race -timeout 10m ./tests/integration/...

# Run integration tests against both PG17 and PG18 sequentially
test-all: test-pg17 test-pg18

# ── Storj end-to-end tests ────────────────────────────────────────────────────
# Requires real Storj credentials AND a running PostgreSQL.
# Tests are skipped automatically when env vars are absent.
#
# Usage:
#   STORJ_ACCESS="<grant>" STORJ_BUCKET="my-bucket" make test-storj
#   STORJ_ACCESS="<grant>" STORJ_BUCKET="my-bucket" PGURL="..." make test-storj

test-storj:
	PGURL="$(PGURL)" \
	STORJ_ACCESS="$(STORJ_ACCESS)" \
	STORJ_API_KEY="$(STORJ_API_KEY)" \
	STORJ_SATELLITE="$(STORJ_SATELLITE)" \
	STORJ_PASSPHRASE="$(STORJ_PASSPHRASE)" \
	STORJ_BUCKET="$(STORJ_BUCKET)" \
	CLUSTER="$(CLUSTER)" \
	WAL_PREFIX="$(WAL_PREFIX)" \
	go test -tags integration -v -race -timeout 30m ./tests/integration/... -run TestStorj

# ── WAL archiving Docker workflow ─────────────────────────────────────────────
# Requires a .env file (or env vars) with STORJ_ACCESS and STORJ_BUCKET set.
#
# Workflow:
#   1. make wal-build             — build the postgres17-wal image (first time / after code changes)
#   2. make wal-up                — start postgres17-wal with archive_mode=on
#   3. Insert data, generate WAL traffic
#   4. Take a base backup:
#        cloud-dump backup --name wal-test \
#          --db-url "postgres://repl_user:repl_password@localhost:5436/postgres?replication=yes" \
#          --storage storj --storj-access "$$STORJ_ACCESS" --storj-bucket "$$STORJ_BUCKET"
#   5. Insert more data (these changes land only in archived WAL)
#   6. make wal-restore-reset     — wipe the restore target directory
#   7. cloud-dump restore with --recovery-target-time (or without for full replay):
#        cloud-dump restore --name wal-test \
#          --pgdata docker/restore-data/pg17-wal \
#          --storage storj --storj-access "$$STORJ_ACCESS" --storj-bucket "$$STORJ_BUCKET" \
#          --recovery-target-time "2026-03-22 10:00:00 UTC"
#   8. make wal-restore-start     — start postgres17-wal-restore; it replays WAL and promotes
#
# The restore_command is embedded in postgresql.auto.conf by cloud-dump restore.
# Alternatively: use restore_command = '/usr/local/bin/wal-restore %f %p'
# and supply STORJ_ACCESS + STORJ_BUCKET as env vars in the container.

wal-build:
	$(DOCKER_COMPOSE) build postgres17-wal postgres17-wal-restore

wal-up:
	$(DOCKER_COMPOSE) up -d postgres17-wal
	@$(MAKE) wal-wait

wal-wait:
	@echo "Waiting for postgres17-wal (localhost:5436)..."
	@for i in $$(seq 1 30); do \
		$(DOCKER_COMPOSE) exec -T postgres17-wal pg_isready -U postgres -q && break; \
		echo "  attempt $$i/30 — postgres17-wal not ready"; \
		sleep 3; \
	done
	@$(DOCKER_COMPOSE) exec -T postgres17-wal pg_isready -U postgres -q \
		|| (echo "postgres17-wal did not become ready in time"; exit 1)
	@echo "postgres17-wal ready on localhost:5436"

wal-down:
	$(DOCKER_COMPOSE) stop postgres17-wal postgres17-wal-restore

wal-restore-reset:
	@echo "Stopping postgres17-wal-restore..."
	$(DOCKER_COMPOSE) stop postgres17-wal-restore
	@echo "Wiping docker/restore-data/pg17-wal/ ..."
	docker run --rm \
	  -v "$(CURDIR)/docker/restore-data/pg17-wal:/data" \
	  alpine:latest \
	  sh -c "rm -rf /data/* /data/.[!.]* 2>/dev/null; true"
	@echo "Ready. Run: cloud-dump restore --pgdata docker/restore-data/pg17-wal ..."
	@echo "Then:  make wal-restore-start"

wal-restore-start:
	$(DOCKER_COMPOSE) up -d postgres17-wal-restore
	@$(MAKE) wal-restore-wait

wal-restore-wait:
	@echo "Waiting for postgres17-wal-restore (localhost:5438) — WAL replay in progress..."
	@for i in $$(seq 1 60); do \
		$(DOCKER_COMPOSE) exec -T postgres17-wal-restore pg_isready -U postgres -q && break; \
		echo "  attempt $$i/60 — replaying WAL..."; \
		sleep 5; \
	done
	@$(DOCKER_COMPOSE) exec -T postgres17-wal-restore pg_isready -U postgres -q \
		|| (echo "postgres17-wal-restore did not become ready in time"; exit 1)
	@echo "postgres17-wal-restore ready on localhost:5438"

# ── Full test suite ───────────────────────────────────────────────────────────

test: docker-up test-unit test-all

# ── CI target ─────────────────────────────────────────────────────────────────

ci: docker-up test-unit test-all docker-down
