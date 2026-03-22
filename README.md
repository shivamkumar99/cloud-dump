# cloud-dump

A Go CLI that streams complete physical PostgreSQL backups directly to cloud storage — no `pg_dump`, no temporary files on disk. Supports **WAL archiving** and **Point-in-Time Recovery (PITR)**.

Uses PostgreSQL's native **streaming replication protocol** (same as `pg_basebackup`, pure Go). Captures the entire cluster: all databases, roles, permissions, sequences, functions, and triggers.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Storage Layout on Storj](#storage-layout-on-storj)
- [Backup & Restore Scenarios](#backup--restore-scenarios)
  - [Case 1: Base backup only (no WAL archiving)](#case-1-base-backup-only-no-wal-archiving)
  - [Case 2: One-time backup + WAL archiving](#case-2-one-time-backup--wal-archiving)
  - [Case 3: Periodic backups + WAL archiving (production)](#case-3-periodic-backups--wal-archiving-production)
- [Migration Note (v1 path change)](#migration-note-v1-path-change)
- [Testing Guide](#testing-guide)
  - [Unit tests (no Docker)](#unit-tests-no-docker)
  - [Integration tests by concern](#integration-tests-by-concern)
  - [Storj end-to-end tests (real cloud storage)](#storj-end-to-end-tests-real-cloud-storage)
  - [Manual CLI testing](#manual-cli-testing)
- [CLI Reference](#cli-reference)
  - [backup](#backup)
  - [restore](#restore)
  - [list](#list)
  - [wal-push](#wal-push)
  - [wal-fetch](#wal-fetch)
  - [Global flags](#global-flags)
- [Environment Variables](#environment-variables)
- [WAL Archiving & PITR](#wal-archiving--pitr)
- [Encryption](#encryption)
- [How it works](#how-it-works)
- [Development environment](#development-environment)
- [Project structure](#project-structure)

---

## Quick Start

```bash
# 1. Clone and build
git clone https://github.com/shivamkumar99/cloud-dump
cd cloud-dump
make build

# 2. Build the WAL image + start all databases + pgAdmin
make wal-build
make docker-up

# 3. Run all tests — unit + integration (no Storj account needed)
make test
```

---

## Features

| Feature | Details |
|---|---|
| Physical backup | `BASE_BACKUP` replication protocol — no SQL on data path, minimal server load |
| WAL archiving | Continuous WAL archiving via `archive_command` for Point-in-Time Recovery |
| PITR | Restore to any timestamp or LSN after a base backup using archived WAL |
| Zero disk writes | Go pipes stream: Postgres → gzip → encryption → cloud storage |
| Parallel uploads | Goroutine pool per tablespace, bounded by `--parallel` |
| Optional encryption | [`filippo.io/age`](https://age-encryption.org) — passphrase or X25519 key-pair |
| Idempotent WAL push | Skips upload if WAL segment already exists in storage |
| Cluster layout | `--cluster` groups all backups and WAL under one namespace |
| Extensible storage | `Storage` interface — add S3 / Azure without touching backup logic |

---

## Requirements

| Dependency | Version |
|---|---|
| Go | 1.22+ |
| PostgreSQL | 10+ (server being backed up) |
| Docker + Compose | Any recent version (local dev / integration tests) |
| Storj | Access grant + bucket (WAL archiving and production use — not needed for unit/integration tests) |

**PostgreSQL one-time setup** on the server being backed up:

```sql
CREATE USER repl_user WITH REPLICATION ENCRYPTED PASSWORD 'repl_password';
```

```
# pg_hba.conf
host  replication  repl_user  0.0.0.0/0  scram-sha-256
```

```sql
SELECT pg_reload_conf();
```

---

## Installation

```bash
# From source
git clone https://github.com/shivamkumar99/cloud-dump
cd cloud-dump
go build -o cloud-dump .

# Or install directly
go install github.com/shivamkumar99/cloud-dump@latest
```

---

## Storage Layout on Storj

All objects live inside a single Storj bucket. The folder structure depends on whether you use `--cluster`.

### Without `--cluster` (flat layout)

```
bucket/
├── <backup-name>/
│   ├── manifest.json
│   └── base.tar.gz           # unencrypted
│   # or base.tar.gz.age      # passphrase / key-pair encrypted
└── wal_archive/              # default --wal-prefix
    ├── 000000010000000000000001.gz
    ├── 000000010000000000000002.gz
    └── ...
```

Backups and WAL use separate, independent prefixes. You supply `--wal-prefix` on every `wal-push` and `wal-fetch` call.

### With `--cluster <name>` (recommended for production)

```
bucket/
└── <cluster-name>/
    ├── backup/
    │   ├── 2026-03-20/
    │   │   ├── manifest.json
    │   │   └── base.tar.gz
    │   ├── 2026-03-21/
    │   │   ├── manifest.json
    │   │   └── base.tar.gz
    │   └── 2026-03-22/
    │       ├── manifest.json
    │       └── base.tar.gz
    └── wal_archive/
        ├── 000000010000000000000001.gz
        ├── 000000010000000000000002.gz
        └── ...
```

`--cluster` automatically derives:
- Backup path: `<cluster>/backup/<name>/`
- WAL path: `<cluster>/wal_archive/`

No extra flags needed — just pass `--cluster` once and all commands use the right prefixes.

---

## Backup & Restore Scenarios

### Case 1: Base backup only (no WAL archiving)

WAL archiving is **not** enabled in `postgresql.conf`. You only run `cloud-dump backup` periodically.

**What is stored:**
```
bucket/
└── prod-2026-03-22/
    ├── manifest.json     ← StartLSN, EndLSN, PG version, encryption flag
    └── base.tar.gz       ← complete PGDATA snapshot
```

**What you can restore to:**

| Restore target | Possible? | How |
|---|---|---|
| Exact state at backup time (EndLSN) | ✅ Yes | `cloud-dump restore --name prod-2026-03-22 --pgdata /data` |
| Any point after EndLSN | ❌ No | WAL archive required |
| Any point before EndLSN | ❌ No | The backup is only consistent at EndLSN |

> **Why can't you restore to a point between StartLSN and EndLSN?**
> A base backup is taken from a *live, running* database. Data pages in the tar are a mix — some written before StartLSN, some during. The database only reaches a consistent state at EndLSN (the `STOP WAL LOCATION` recorded in `backup_label`). Stopping WAL replay before EndLSN leaves data files inconsistent. PostgreSQL itself enforces this: it will not promote before reaching the backup's stop location.

**Restore command:**
```bash
# Plain restore — reaches EndLSN automatically, no PITR flags needed
cloud-dump restore \
  --name prod-2026-03-22 \
  --pgdata /var/lib/postgresql/data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

pg_ctl start -D /var/lib/postgresql/data
```

---

### Case 2: One-time backup + WAL archiving

WAL archiving is enabled. You took **one** base backup and WAL segments are being archived continuously after it.

> **WAL archiving alone is useless without a base backup.** WAL is a stream of changes — it has nothing to apply to without a starting PGDATA state. Always pair WAL archiving with at least one base backup.

**What is stored:**
```
bucket/
├── prod-2026-03-22/
│   ├── manifest.json     ← StartLSN: 0/1000000, EndLSN: 0/2000000
│   └── base.tar.gz
└── wal_archive/
    ├── 000000010000000000000001.gz   ← WAL before backup (not needed for restore)
    ├── 000000010000000000000002.gz   ← WAL from backup onward
    └── ...                           ← continuous stream up to NOW
```

**What you can restore to:**

| Restore target | Possible? | How |
|---|---|---|
| Exact state at backup time (EndLSN) | ✅ Yes | plain restore, no `--recovery-target-*` |
| Any point from EndLSN to NOW | ✅ Yes | `--recovery-target-time` or `--recovery-target-lsn` |
| Any point before EndLSN | ❌ No | not consistent before EndLSN |

**Restore commands:**
```bash
# Plain restore to backup time
cloud-dump restore --name prod-2026-03-22 --pgdata /data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

# PITR — restore to a specific timestamp after the backup
cloud-dump restore --name prod-2026-03-22 --pgdata /data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --recovery-target-time "2026-03-22 14:30:00 UTC"

# PITR — restore to a specific LSN after the backup
cloud-dump restore --name prod-2026-03-22 --pgdata /data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --recovery-target-lsn "0/5200000"

pg_ctl start -D /data   # PostgreSQL replays WAL then promotes automatically
```

---

### Case 3: Periodic backups + WAL archiving (production)

WAL archiving is always on. You take a new base backup daily or hourly. This is the full production setup.

Use `--cluster` to keep everything organised under one namespace:

```bash
# Daily backup cron job
cloud-dump backup \
  --cluster prod-pg17 \
  --name $(date +%Y-%m-%d) \
  --db-url "postgres://repl_user:pass@localhost:5432/postgres?replication=yes" \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

# postgresql.conf — WAL archiving
archive_command = 'cloud-dump wal-push %p %f --cluster prod-pg17 \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups'
```

**What is stored:**
```
bucket/
└── prod-pg17/
    ├── backup/
    │   ├── 2026-03-20/
    │   │   ├── manifest.json     ← StartLSN: 0/1000000, EndLSN: 0/2000000
    │   │   └── base.tar.gz
    │   ├── 2026-03-21/
    │   │   ├── manifest.json     ← StartLSN: 0/5000000, EndLSN: 0/6000000
    │   │   └── base.tar.gz
    │   └── 2026-03-22/
    │       ├── manifest.json     ← StartLSN: 0/9000000, EndLSN: 0/A000000
    │       └── base.tar.gz
    └── wal_archive/
        ├── 000000010000000000000010.gz   ← WAL from before 2026-03-20 backup
        ├── ...
        ├── 000000010000000000000050.gz   ← WAL spanning all three backups
        └── ...                           ← continues up to NOW
```

**What you can restore to:**

| Restore target | Which base backup to use | Possible? |
|---|---|---|
| Exact state of any backup | That backup's name | ✅ Yes |
| Any point after any backup's EndLSN | The most recent backup before your target | ✅ Yes |

> Always use the **most recent base backup whose EndLSN is before your target time**. Using an older backup works too but replays more WAL (slower).

**List available backups:**
```bash
cloud-dump list --cluster prod-pg17 \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups
```
Output:
```
Cluster: prod-pg17  (WAL archive: prod-pg17/wal_archive/)

NAME                  TIMESTAMP                  ENCRYPTED   PG VERSION
----------------------------------------------------------------------------------------------------
2026-03-22            2026-03-22 02:00:00 UTC    no          17.2
2026-03-21            2026-03-21 02:00:01 UTC    no          17.2
2026-03-20            2026-03-20 02:00:02 UTC    no          17.2
```

**Restore commands:**
```bash
# Restore to NOW (latest archived WAL) using most recent backup
cloud-dump restore --cluster prod-pg17 --name 2026-03-22 --pgdata /data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --recovery-target-lsn "0/FFFFFFFF"   # or just latest

# PITR — restore to a specific time on 2026-03-21
cloud-dump restore --cluster prod-pg17 --name 2026-03-21 --pgdata /data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --recovery-target-time "2026-03-21 15:30:00 UTC"

pg_ctl start -D /data
```

---

## Migration Note (v1 path change)

> **If you stored backups or WAL before this change, read this.**

The cluster-mode storage paths changed:

| What | Old path | New path |
|---|---|---|
| Backups | `<cluster>/backups/<name>/` | `<cluster>/backup/<name>/` |
| WAL archive | `<cluster>/wal/` | `<cluster>/wal_archive/` |

This **only affects the `--cluster` flag**. The flat layout (no `--cluster`) is unchanged.

**How to migrate existing objects in Storj:**

You need to copy objects from the old paths to the new paths. Use the Storj web console, `uplink` CLI, or any S3-compatible tool:

```bash
# Using uplink CLI — copy all backups
uplink cp --recursive \
  sj://my-bucket/prod-pg17/backups/ \
  sj://my-bucket/prod-pg17/backup/

# Copy WAL archive
uplink cp --recursive \
  sj://my-bucket/prod-pg17/wal/ \
  sj://my-bucket/prod-pg17/wal_archive/

# After verifying the new paths work, delete the old ones
uplink rm --recursive sj://my-bucket/prod-pg17/backups/
uplink rm --recursive sj://my-bucket/prod-pg17/wal/
```

Verify the migration worked before deleting the old paths:
```bash
cloud-dump list --cluster prod-pg17 \
  --storage storj --storj-access "<grant>" --storj-bucket my-bucket
```

---

## Testing Guide

All standard integration tests use **in-memory storage** — no Storj account required.

### Unit tests (no Docker)

No external services needed. Covers WAL logic, crypto, storage, and restore pipeline.

```bash
make test-unit
```

Run only WAL unit tests:

```bash
make test-wal-unit
```

**WAL unit tests (`internal/pgbackup/wal_test.go`, `internal/storage/memory_test.go`):**

| Test | What it verifies |
|---|---|
| `TestWalPush_Uncompressed` | Stored bytes are identical to source; no `.gz` suffix added |
| `TestWalPush_Compressed` | Stored data starts with gzip magic bytes (`0x1f 0x8b`) |
| `TestWalPush_PassphraseEncrypted` | Stored under `.gz.age` key; content not readable as gzip |
| `TestWalPush_KeyPairEncrypted` | Stored under `.gz.age` key using X25519 key-pair |
| `TestWalPush_Idempotent` | Second push of same segment exits 0 and does not corrupt stored content |
| `TestWalFetch_Uncompressed` | Retrieved bytes match original exactly |
| `TestWalFetch_Compressed` | Decompressed content matches original |
| `TestWalFetch_PassphraseEncrypted` | Decrypted + decompressed content matches original |
| `TestWalFetch_KeyPairEncrypted` | Full round-trip with freshly generated X25519 key pair |
| `TestWalFetch_NotFound` | Returns `ErrWalNotFound`; destination file is **not** written |
| `TestWalFetch_WrongPassphrase` | Returns error; destination file is **not** written |

---

### Integration tests by concern

Requires a running PostgreSQL — start it first:

```bash
make docker-up
```

Then run any of these targeted commands:

| Command | What it tests |
|---|---|
| `make test-integration` | All integration tests |
| `make test-backup` | Backup only |
| `make test-restore` | Restore only |
| `make test-wal` | WAL push + fetch only |

---

#### Backup integration tests (`make test-backup`)

Tests in `tests/integration/backup_test.go`. All use in-memory storage connected to the real Docker PostgreSQL.

| Test | What it verifies |
|---|---|
| `TestBackup_CreatesManifest` | `manifest.json` is written with correct backup name, PG version, system ID, StartLSN, EndLSN |
| `TestBackup_ManifestRoundTrip` | Manifest written then read back — all fields match |
| `TestBackup_CreatesBaseTar` | `base.tar.gz` object is created and contains actual tar entries |
| `TestBackup_BaseTarContainsPGData` | Tar entries include `PG_VERSION`, `global/`, `base/` — confirms real PGDATA was streamed |
| `TestBackup_Encrypted_Passphrase` | Encrypted backup stored under `.age` suffix; not readable as gzip (ciphertext check) |
| `TestBackup_Encrypted_KeyPair` | X25519 key-pair encrypted backup stored under `.age` suffix |
| `TestBackup_NoCollision` | Two consecutive backups with distinct names produce no shared keys in storage |

---

#### Restore integration tests (`make test-restore`)

Tests in `tests/integration/restore_test.go`. Backup + restore using in-memory storage; PGDATA extracted to temp dirs.

**Case 1 — plain restore (no WAL archive):**

| Test | What it verifies |
|---|---|
| `TestRestore_Basic` | Full backup → restore: `PG_VERSION`, `global/`, `base/` present in PGDATA; **`recovery.signal` must NOT be written** (plain restore uses `backup_label` for crash recovery — writing `recovery.signal` without `restore_command` causes PostgreSQL FATAL) |
| `TestRestore_Passphrase_Encrypted` | Passphrase-encrypted backup → restore with matching passphrase: PGDATA intact, no `recovery.signal` |
| `TestRestore_KeyPair_Encrypted` | X25519 encrypted backup → restore with matching private key: PGDATA intact, no `recovery.signal` |
| `TestRestore_WrongPassphrase` | Restore with wrong passphrase returns error — backup is not silently corrupted |
| `TestRestore_DownloadApplySplit` | Two-phase restore: `Download()` populates staging dir → `Apply()` extracts to PGDATA separately. Verifies staging dir has at least one blob and manifest fields are correct |
| `TestRestore_PlainNoRecoverySignal` | Explicit assertion: plain restore (no `--recovery-target-*`) must never write `recovery.signal` |

**PITR configuration tests:**

| Test | What it verifies |
|---|---|
| `TestRestore_PITRConfig` | Restore with all three PITR flags: verifies `postgresql.auto.conf` contains `restore_command`, `recovery_target_time`, `recovery_target_lsn`, and `recovery_target_action = 'promote'`; verifies `recovery.signal` **is** written |

**Cluster layout tests:**

| Test | What it verifies |
|---|---|
| `TestBackup_ClusterLayout` | Backup with `BackupName = <cluster>/backup/<name>` → objects stored at `<cluster>/backup/<name>/manifest.json` and `base.tar.gz`; no objects at the bare name path; WAL pushed to `<cluster>/wal_archive/` is stored at correct key |
| `TestRestore_ClusterLayout` | Backup + restore using full cluster key path; PITR config in `postgresql.auto.conf` references the cluster WAL prefix; `recovery.signal` is written |

---

#### WAL integration tests (`make test-wal`)

Tests in `tests/integration/wal_test.go`. Use in-memory storage — no Storj needed.

| Test | What it verifies |
|---|---|
| `TestWal_Push_Uncompressed` | Stored bytes identical to source; no `.gz` suffix |
| `TestWal_Push_Compressed` | Stored data starts with gzip magic bytes |
| `TestWal_Push_PassphraseEncrypted` | Stored under `.gz.age` key; not readable as gzip |
| `TestWal_Push_KeyPairEncrypted` | X25519 encrypted; stored under `.gz.age` key |
| `TestWal_Push_Idempotent` | Second push exits 0 and stored content unchanged; byte-for-byte verify after both pushes |
| `TestWal_Fetch_Uncompressed` | Retrieved bytes match original |
| `TestWal_Fetch_Compressed` | Decompressed content matches original |
| `TestWal_Fetch_PassphraseEncrypted` | Decrypted + decompressed content matches original |
| `TestWal_Fetch_KeyPairEncrypted` | Full X25519 round-trip; decrypted content matches original |
| `TestWal_Fetch_NotFound` | Returns `ErrWalNotFound`; destination file not written |
| `TestWal_Fetch_WrongPassphrase` | Returns error; destination file not written |
| `TestWal_MultiSegment_PushFetch` | Push N sequential segments → fetch each back → every segment byte-for-byte correct |

---

### Storj end-to-end tests (real cloud storage)

The standard integration tests use **in-memory storage** — files never leave your machine. The Storj tests run the full pipeline against a real Storj bucket.

**What you need:**
- A Storj account with a bucket and credentials (access grant or API key)
- A running PostgreSQL (`make docker-up`)
- A `.env` file (copy from `.env.example`)

**Run:**

```bash
make docker-up   # if not already running
make test-storj
```

Storj tests auto-skip when credentials are missing:
```
--- SKIP: TestStorj_Backup_And_Restore (0.00s)
    storj_test.go:75: set STORJ_BUCKET and either STORJ_ACCESS or ...
```

**Cleanup:** each test deletes all objects it wrote via `t.Cleanup` — reruns start clean.

---

#### Storj test descriptions

**Scenario 1 — Base backup only (no WAL):**

| Test | Case | What it verifies |
|---|---|---|
| `TestStorj_Backup_And_Restore` | Case 1 | Backup to real Storj bucket → `manifest.json` + `base.tar.gz` exist at correct keys → restore to temp PGDATA dir → `PG_VERSION`, `global/`, `base/` present → **no `recovery.signal`** (plain restore) |
| `TestStorj_Backup_Encrypted_Restore` | Case 1 encrypted | Passphrase-encrypted backup stored under `.age` key → not readable as gzip → restore with correct passphrase succeeds → no `recovery.signal` |

**Scenario 2 — Backup + data verification (restore to Docker target):**

| Test | Case | What it verifies |
|---|---|---|
| `TestStorj_Backup_Restore_DataVerify` | Case 1 | Records source row counts (`inventory_db.items`, `inventory_db.warehouses`, `ecommerce_db.users`) before backup → restores to `postgres17-restore` container (port 5433) → queries restored DB → counts match exactly → `restore_marker` table from pre-restore init is gone (confirms real backup was used, not original container data) |
| `TestStorj_Backup_Encrypted_Restore_DataVerify` | Case 1 encrypted | Same as above but with passphrase encryption — decryption is transparent to row count verification |

**Scenario 3 — WAL archiving + PITR (Case 2):**

| Test | Case | What it verifies |
|---|---|---|
| `TestStorj_WAL_PITR` | Case 2 | Full PITR cycle against `postgres17-wal` (port 5436): (1) base backup; (2) create table + insert 10 "before" rows; (3) force WAL switch + wait for **named segment** to be archived to Storj; (4) record `beforeTime`; (5) insert 5 "after" rows + archive their segment. Sub-test **BeforeInsert**: restore with `recovery_target_time = beforeTime` → start `postgres17-wal-restore` → wait for full promotion (`pg_is_in_recovery() = false`) → verify 10 before rows and 0 after rows. Sub-test **AfterInsert**: restore with `recovery_target_lsn = afterLSN` → promote → verify 10 before + 5 after rows |

**Scenario 4 — WAL push / fetch (individual operations):**

| Test | What it verifies |
|---|---|
| `TestStorj_Wal_PushFetch` | WAL segment pushed to Storj under `.gz` key → fetched back → byte-for-byte identical to original |
| `TestStorj_Wal_PushFetch_Encrypted` | Passphrase-encrypted WAL stored under `.gz.age` key; not readable as gzip → fetched + decrypted → matches original |
| `TestStorj_Wal_Idempotent` | Push same segment twice → both exit 0 → stored content unchanged → fetched content matches original |

**Scenario 5 — Cluster folder structure (Case 3):**

| Test | What it verifies |
|---|---|
| `TestStorj_ClusterWALLayout` | WAL pushed with `WalPrefix = <cluster>/wal_archive` → stored at `<cluster>/wal_archive/<segment>.gz`; backup with `BackupName = <cluster>/backup/test-backup` → stored at `<cluster>/backup/test-backup/manifest.json` and `base.tar.gz`; `List()` of all objects under cluster root → every key starts with `<cluster>/`; confirms both `/wal_archive/` and `/backup/` sub-trees are present; logs the full path tree |

---

### Manual CLI testing

Build the binary and start Docker:

```bash
make build && make docker-up
```

**Test backup:**

```bash
./cloud-dump backup \
  --db-url "postgres://repl_user:repl_password@localhost:5432/postgres?replication=yes" \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --name test-$(date +%Y%m%d-%H%M%S)
```

**Test list:**

```bash
# Flat layout
./cloud-dump list --storage storj --storj-access "<grant>" --storj-bucket my-backups

# Cluster layout
./cloud-dump list --cluster prod-pg17 --storage storj --storj-access "<grant>" --storj-bucket my-backups
```

**Test restore (to temp dir):**

```bash
mkdir -p /tmp/pgdata-test

./cloud-dump restore \
  --name <backup-name-from-list> \
  --pgdata /tmp/pgdata-test \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

# No recovery.signal for plain restore
ls /tmp/pgdata-test/PG_VERSION
ls /tmp/pgdata-test/recovery.signal   # should not exist
```

**Test WAL push / fetch:**

```bash
# Create a fake WAL segment
dd if=/dev/urandom of=/tmp/000000010000000000000001 bs=1M count=16

# Push (compressed, default)
./cloud-dump wal-push /tmp/000000010000000000000001 000000010000000000000001 \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

# Fetch back
./cloud-dump wal-fetch 000000010000000000000001 /tmp/fetched \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

# Verify byte-for-byte match
diff /tmp/000000010000000000000001 /tmp/fetched && echo "MATCH"

# Idempotency — push again, must exit 0
./cloud-dump wal-push /tmp/000000010000000000000001 000000010000000000000001 \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups
echo "Exit: $?"   # must be 0

# Not-found — must exit 1
./cloud-dump wal-fetch 000000010000000000000099 /tmp/nope \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups
echo "Exit: $?"   # must be 1
```

**Test restore to Docker target:**

```bash
make restore17-reset   # wipe docker/restore-data/pg17/

./cloud-dump restore \
  --name <backup-name-from-pg17> \
  --pgdata docker/restore-data/pg17 \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

make restore17-start   # start postgres17-restore on port 5433

psql "postgres://postgres:postgres@localhost:5433/postgres" -c "\l"
```

---

## CLI Reference

### backup

Stream a complete physical backup of the PostgreSQL cluster to cloud storage.

```
cloud-dump backup --name <name> [flags]
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--name` | Yes | — | Unique identifier for this backup |
| `--db-url` | Yes | — | PostgreSQL DSN with `?replication=yes` |
| `--cluster` | No | — | Groups backups under `<cluster>/backup/<name>/` |
| `--encrypt` | No | `false` | Enable encryption |
| `--passphrase` | No | — | Encryption passphrase (with `--encrypt`) |
| `--recipient-key` | No | — | Path to age public key file (with `--encrypt`) |
| `--parallel` | No | `4` | Parallel upload goroutines |

```bash
# Unencrypted — flat layout
cloud-dump backup \
  --db-url "postgres://repl_user:repl_password@localhost:5432/postgres?replication=yes" \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --name prod-2026-03-22

# With cluster — stored at prod-pg17/backup/2026-03-22/
cloud-dump backup \
  --db-url "postgres://repl_user:repl_password@localhost:5432/postgres?replication=yes" \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --cluster prod-pg17 --name 2026-03-22

# Passphrase-encrypted
cloud-dump backup ... --encrypt --passphrase "my-strong-passphrase"

# Key-pair encrypted (public key on backup server; private key stored securely elsewhere)
cloud-dump backup ... --encrypt --recipient-key ~/.config/cloud-dump/age.key.pub
```

---

### restore

Download a backup and extract it to an empty PGDATA directory. After restore, start PostgreSQL — it replays WAL and promotes automatically.

```
cloud-dump restore --name <name> --pgdata <dir> [flags]
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--name` | Yes | — | Backup name to restore |
| `--pgdata` | Yes | — | Target PGDATA directory (must be empty) |
| `--cluster` | No | — | Cluster prefix — looks up `<cluster>/backup/<name>/` |
| `--passphrase` | No | — | Decryption passphrase |
| `--identity-key` | No | — | Path to age private key |
| `--recovery-target-time` | No | — | PITR: stop at this timestamp (requires WAL archive) |
| `--recovery-target-lsn` | No | — | PITR: stop at this LSN (requires WAL archive) |
| `--wal-prefix` | No | `wal_archive` | Storage prefix for WAL archive (flat layout only) |
| `--wal-passphrase` | No | — | Passphrase for encrypted WAL files |
| `--wal-identity-key` | No | — | Private key for encrypted WAL files |

```bash
# Case 1 — plain restore (no PITR)
cloud-dump restore \
  --name prod-2026-03-22 --pgdata /var/lib/postgresql/data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups

# Case 2/3 — PITR to a specific timestamp
cloud-dump restore \
  --cluster prod-pg17 --name 2026-03-21 --pgdata /var/lib/postgresql/data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --recovery-target-time "2026-03-21 15:30:00 UTC"

# Case 2/3 — PITR to a specific LSN
cloud-dump restore \
  --cluster prod-pg17 --name 2026-03-21 --pgdata /var/lib/postgresql/data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --recovery-target-lsn "0/5200000"

# Start PostgreSQL after any restore
pg_ctl start -D /var/lib/postgresql/data
```

---

### list

List all backups in cloud storage. With `--cluster`, also shows the WAL archive path.

```
cloud-dump list [flags]
```

```bash
# Flat layout
cloud-dump list --storage storj --storj-access "<grant>" --storj-bucket my-backups

# Cluster layout
cloud-dump list --cluster prod-pg17 --storage storj --storj-access "<grant>" --storj-bucket my-backups
```

Output (cluster mode):
```
Cluster: prod-pg17  (WAL archive: prod-pg17/wal_archive/)

NAME                  TIMESTAMP                  ENCRYPTED   PG VERSION
----------------------------------------------------------------------------------------------------
2026-03-22            2026-03-22 02:00:00 UTC    no          17.2
2026-03-21            2026-03-21 02:00:01 UTC    no          17.2
```

---

### wal-push

Archive a single WAL segment to cloud storage. Used as PostgreSQL's `archive_command`. Idempotent — skips upload if segment already exists.

```
cloud-dump wal-push <wal-file-path> <wal-file-name> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--compress` | `true` | Compress with gzip before upload |
| `--wal-prefix` | `wal_archive` | Storage key prefix (flat layout) |
| `--cluster` | — | Derives WAL prefix as `<cluster>/wal_archive/` |
| `--encrypt` | `false` | Enable encryption |
| `--passphrase` | — | Encryption passphrase |
| `--recipient-key` | — | Path to age public key file |

Exit 0 → archived. Non-zero → PostgreSQL retries.

**`postgresql.conf`:**

```ini
wal_level = replica
archive_mode = on

# Flat layout
archive_command = 'cloud-dump wal-push %p %f --storage storj --storj-access "<grant>" --storj-bucket my-backups'

# Cluster layout (WAL goes to prod-pg17/wal_archive/)
archive_command = 'cloud-dump wal-push %p %f --cluster prod-pg17 --storage storj --storj-access "<grant>" --storj-bucket my-backups'

# Encrypted WAL
archive_command = 'cloud-dump wal-push %p %f --encrypt --passphrase "wal-secret" --cluster prod-pg17 --storage storj --storj-access "<grant>" --storj-bucket my-backups'
```

---

### wal-fetch

Fetch a single WAL segment from cloud storage. Used as PostgreSQL's `restore_command` during recovery.

```
cloud-dump wal-fetch <wal-file-name> <destination-path> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--passphrase` | — | Decryption passphrase |
| `--identity-key` | — | Path to age private key file |
| `--wal-prefix` | `wal_archive` | Storage key prefix (flat layout) |
| `--cluster` | — | Derives WAL prefix as `<cluster>/wal_archive/` |

Exit 0 → segment fetched, PostgreSQL replays it. Exit 1 → not found, PostgreSQL promotes.

`cloud-dump restore` writes this automatically into `postgresql.auto.conf` when PITR flags are set.

---

### Global flags

| Flag | Default | Description |
|---|---|---|
| `--db-url` | — | PostgreSQL DSN with `?replication=yes` (backup only) |
| `--cluster` | — | Cluster namespace — see [Storage Layout](#storage-layout-on-storj) |
| `--storage` | `storj` | Storage backend |
| `--storj-access` | — | Storj serialised access grant |
| `--storj-api-key` | — | Storj API key (alternative to `--storj-access`) |
| `--storj-satellite` | — | Storj satellite address |
| `--storj-passphrase` | — | Storj encryption passphrase |
| `--storj-bucket` | — | Storj bucket name |
| `--parallel` | `4` | Goroutines for parallel upload / download |
| `--log-level` | `info` | `debug` / `info` / `warn` / `error` |

> **Storj auth** — use either `--storj-access` (single serialised grant) or `--storj-api-key` + `--storj-satellite` + `--storj-passphrase`.

---

## Environment Variables

Every flag has a corresponding environment variable. **CLI flag always takes priority.**

### Storj storage credentials

**Option A — Access grant (recommended)**

| Environment variable | Equivalent flag | Description |
|---|---|---|
| `STORJ_ACCESS` | `--storj-access` | Serialised access grant (satellite + API key + passphrase in one value) |
| `STORJ_BUCKET` | `--storj-bucket` | Bucket name |

**Option B — API key + satellite + passphrase**

| Environment variable | Equivalent flag | Description |
|---|---|---|
| `STORJ_API_KEY` | `--storj-api-key` | Storj API key |
| `STORJ_SATELLITE` | `--storj-satellite` | Satellite address, e.g. `121RTSDp...@ap1.storj.io:7777` |
| `STORJ_PASSPHRASE` | `--storj-passphrase` | Storj encryption passphrase (Storj-level, not backup content) |
| `STORJ_BUCKET` | `--storj-bucket` | Bucket name |

### Database connection

| Environment variable | Equivalent flag | Description |
|---|---|---|
| `CLOUD_DUMP_DB_URL` | `--db-url` | PostgreSQL connection URL with `replication=yes` |
| `CLOUD_DUMP_CLUSTER` | `--cluster` | Cluster name |

### WAL archiving

| Environment variable | Equivalent flag | Commands | Description |
|---|---|---|---|
| `WAL_COMPRESS` | `--compress` | `wal-push` | Set to `false` to disable gzip. Default: `true` |
| `WAL_ENCRYPT` | `--encrypt` | `wal-push` | Set to `true` to encrypt WAL segments |
| `WAL_PASSPHRASE` | `--passphrase` | `wal-push`, `wal-fetch` | Passphrase for symmetric WAL encryption/decryption |
| `WAL_RECIPIENT_KEY` | `--recipient-key` | `wal-push` | Path to age X25519 public key file |
| `WAL_IDENTITY_KEY` | `--identity-key` | `wal-fetch` | Path to age X25519 private key file |

### Production setup (systemd)

```bash
sudo mkdir -p /etc/systemd/system/postgresql@17-main.service.d/
sudo tee /etc/systemd/system/postgresql@17-main.service.d/cloud-dump.conf > /dev/null <<EOF
[Service]
EnvironmentFile=/etc/cloud-dump/storj.env
EOF

sudo tee /etc/cloud-dump/storj.env > /dev/null <<EOF
STORJ_ACCESS=your-access-grant
STORJ_BUCKET=my-backups
CLOUD_DUMP_CLUSTER=prod-pg17
WAL_ENCRYPT=true
WAL_PASSPHRASE=your-wal-secret
EOF
sudo chmod 600 /etc/cloud-dump/storj.env
sudo chown postgres:postgres /etc/cloud-dump/storj.env

sudo systemctl daemon-reload && sudo systemctl restart postgresql@17-main
```

`postgresql.conf` then only needs:

```ini
wal_level = replica
archive_mode = on
archive_command = 'cloud-dump wal-push %p %f --storage storj'
```

---

## WAL Archiving & PITR

### How it works

A base backup captures a snapshot at one moment in time. WAL segments record every change after that. Archiving WAL continuously lets you restore to **any point in time** after any base backup.

```
Timeline ────────────────────────────────────────────────────────────────►

  Base Backup               WAL segments (archived by archive_command)
  2026-03-22 02:00               │
       │               ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
       ▼               │ 0001 │ │ 0002 │ │ 0003 │ │ 0004 │ ...
  ┌──────────┐         └──────┘ └──────┘ └──────┘ └──────┘
  │ base.tar │                               ▲
  └──────────┘                               │
  (EndLSN)                        Restore to "14:30:00 UTC"
                                  by replaying WAL 0001+0002+partial 0003
```

PostgreSQL only requests WAL segments whose LSN is **after** the base backup's EndLSN. WAL before the base backup is never fetched and can be pruned once a new backup is taken.

### Setup

**1.** Enable archiving in `postgresql.conf`:

```ini
wal_level = replica
archive_mode = on
archive_command = 'cloud-dump wal-push %p %f --cluster prod-pg17 --storage storj --storj-access "<grant>" --storj-bucket my-backups'
```

**2.** Reload PostgreSQL:

```sql
SELECT pg_reload_conf();
```

**3.** Take periodic base backups (daily cron):

```bash
cloud-dump backup \
  --cluster prod-pg17 \
  --name $(date +%Y-%m-%d) \
  --db-url "postgres://repl_user:repl_password@localhost:5432/postgres?replication=yes" \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups
```

### PITR restore

`cloud-dump restore` with `--recovery-target-time` or `--recovery-target-lsn` writes into `postgresql.auto.conf`:

```ini
restore_command = 'cloud-dump wal-fetch %f %p --cluster prod-pg17 --storage storj ...'
recovery_target_time = '2026-03-22 14:30:00 UTC'
recovery_target_action = 'promote'
```

And creates `recovery.signal` so PostgreSQL enters recovery mode on startup.

```bash
cloud-dump restore \
  --cluster prod-pg17 --name 2026-03-22 \
  --pgdata /var/lib/postgresql/data \
  --storage storj --storj-access "<grant>" --storj-bucket my-backups \
  --recovery-target-time "2026-03-22 14:30:00 UTC"

pg_ctl start -D /var/lib/postgresql/data
# PostgreSQL: fetches WAL from prod-pg17/wal_archive/ → replays to 14:30 → promotes
```

---

## Encryption

### Passphrase mode

```bash
cloud-dump backup  ... --encrypt --passphrase "my-secret"
cloud-dump restore ... --passphrase "my-secret"

# WAL
cloud-dump wal-push %p %f ... --encrypt --passphrase "wal-secret"
cloud-dump wal-fetch %f %p  ... --passphrase "wal-secret"
```

### Key-pair mode (recommended for production)

Public key encrypts — only needed on the backup server. Private key decrypts — store it offline or in a secrets manager.

```bash
# Generate a key pair
age-keygen -o ~/.config/cloud-dump/age.key

# Backup — public key only (safe to deploy on backup server)
cloud-dump backup ... --encrypt --recipient-key ~/.config/cloud-dump/age.key.pub

# Restore — private key required (keep this off the backup server)
cloud-dump restore ... --identity-key ~/.config/cloud-dump/age.key
```

---

## How it works

**Backup pipeline:**

```
PostgreSQL (BASE_BACKUP protocol)
  │
  ├─ PGDATA tablespace  ──► io.Pipe ──► gzip ──► [age?] ──► Storj  <name>/base.tar.gz[.age]
  └─ Extra tablespaces  ──► io.Pipe ──► gzip ──► [age?] ──► Storj  <name>/<oid>.tar.gz[.age]
                                                                   + <name>/manifest.json
```

- Main goroutine reads tablespaces sequentially (protocol constraint)
- Upload goroutines run in parallel, bounded by `--parallel`
- `io.Pipe` decouples reading and uploading — nothing is buffered in memory

**WAL archive pipeline:**

```
PostgreSQL writes 16 MB WAL segment
  └─ archive_command ──► wal-push ──► gzip ──► [age?] ──► Storj  <prefix>/<name>.gz[.age]
```

**Restore pipeline:**

```
Storj ──► [age decrypt?] ──► gzip decompress ──► tar.Extract ──► PGDATA/
                                                  write recovery.signal  (PITR only)
                                                  append postgresql.auto.conf  (PITR only)
```

---

## Development environment

### Start everything

```bash
# First time: build the WAL image (includes cloud-dump binary)
make wal-build

# Start all databases + pgAdmin
make docker-up
```

All containers and their ports:

| Container | Port | Purpose |
|---|---|---|
| `postgres17` | `5432` | PG17 source — backup / integration tests |
| `postgres17-restore` | `5433` | Restore target for PG17 backups |
| `postgres17-wal` | `5436` | PG17 with `archive_mode=on` — WAL archiving to Storj |
| `postgres17-wal-restore` | `5438` | PITR restore target — started manually after `cloud-dump restore` |
| `pgadmin` | `5050` | Browser SQL viewer — all servers pre-registered |

> `postgres17-wal-restore` is **not** started by `make docker-up`. Populate it with `cloud-dump restore --recovery-target-time ...` first, then run `make wal-restore-start`.

### Credentials

| Container | Port | User | Password | Notes |
|---|---|---|---|---|
| `postgres17` | `5432` | `postgres` | `postgres` | Superuser |
| `postgres17` | `5432` | `repl_user` | `repl_password` | Replication — use in `--db-url` |
| `postgres17-wal` | `5436` | `postgres` | `postgres` | Superuser |
| `postgres17-wal` | `5436` | `repl_user` | `repl_password` | Replication — use in `--db-url` |
| `postgres17-restore` | `5433` | `postgres` | `postgres` | — |
| `postgres17-wal-restore` | `5438` | `postgres` | `postgres` | — |

**pgAdmin (`http://localhost:5050`)** — email: `admin@admin.com`, password: `admin`

### All make targets

| Command | Description |
|---|---|
| `make build` | Compile `cloud-dump` binary |
| `make vet` | Run `go vet ./...` |
| `make fmt` | Run `gofmt` over all files |
| `make wal-build` | Build the `postgres17-wal` Docker image |
| `make docker-up` | Start all containers |
| `make docker-down` | Stop all containers |
| `make docker-reset` | Stop containers + delete volumes |
| `make restore17-reset` | Stop `postgres17-restore`, wipe `docker/restore-data/pg17/` |
| `make restore17-start` | Start `postgres17-restore` with current restore data |
| `make wal-up` | Start `postgres17-wal` only |
| `make wal-down` | Stop WAL containers |
| `make wal-restore-reset` | Stop `postgres17-wal-restore`, wipe `docker/restore-data/pg17-wal/` |
| `make wal-restore-start` | Start `postgres17-wal-restore` (PITR — replays WAL then promotes) |
| `make test-unit` | Unit tests — no Docker required |
| `make test-wal-unit` | WAL unit tests only |
| `make test-integration` | All integration tests |
| `make test-backup` | Backup integration tests only |
| `make test-restore` | Restore integration tests only |
| `make test-wal` | WAL integration tests only |
| `make test-storj` | Storj end-to-end tests (requires `.env` with credentials) |
| `make test` | `docker-up` + unit + integration (Docker stays running) |
| `make ci` | `docker-up` + unit + integration + `docker-down` |

---

## Project structure

```
cloud-dump/
├── main.go
├── go.mod
├── Makefile
├── .env.example             # Storj credentials template
├── cmd/
│   ├── root.go              # Cobra root + persistent flags + cluster path helpers
│   ├── backup.go            # backup subcommand
│   ├── restore.go           # restore subcommand (PITR support)
│   ├── list.go              # list subcommand
│   ├── wal_push.go          # wal-push (archive_command)
│   └── wal_fetch.go         # wal-fetch (restore_command)
├── internal/
│   ├── pgbackup/
│   │   ├── backup.go        # BASE_BACKUP streaming + goroutine pool
│   │   ├── restore.go       # Download → Apply phases + recovery config
│   │   ├── manifest.go      # Manifest: LSNs, encryption flag, tablespace list
│   │   ├── wal.go           # WalPush / WalFetch
│   │   ├── backup_helpers_test.go
│   │   ├── restore_test.go
│   │   └── wal_test.go
│   ├── crypto/
│   │   ├── crypto.go        # Encryptor interface + Noop / Passphrase / KeyPair
│   │   └── crypto_test.go
│   └── storage/
│       ├── storage.go       # Storage interface + factory
│       ├── storj.go         # Storj uplink implementation
│       ├── memory.go        # In-memory implementation (tests)
│       └── memory_test.go
├── tests/
│   └── integration/
│       ├── helpers_test.go  # Shared test utilities
│       ├── infra_test.go    # Docker + PostgreSQL helpers
│       ├── backup_test.go   # Backup integration tests (in-memory storage)
│       ├── restore_test.go  # Restore integration tests (in-memory storage)
│       ├── wal_test.go      # WAL push + fetch integration tests (in-memory storage)
│       └── storj_test.go    # End-to-end tests against real Storj (skipped without creds)
├── docker/
│   ├── Dockerfile           # Multi-stage: Go build → postgres:17-alpine + cloud-dump binary
│   ├── docker-compose.yml   # All PostgreSQL instances + pgAdmin
│   ├── wal-archive.sh       # archive_command wrapper (reads Storj creds from env)
│   ├── wal-restore.sh       # restore_command wrapper (reads Storj creds from env)
│   └── restore-data/        # Bind-mounted PGDATA dirs for restore containers
│       ├── pg17/            # postgres17-restore PGDATA
│       └── pg17-wal/        # postgres17-wal-restore PGDATA
└── roadmaps/
    └── roadmap.md
```
