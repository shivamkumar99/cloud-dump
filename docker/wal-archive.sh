#!/bin/sh
# WAL archive wrapper — called by PostgreSQL as archive_command.
#
# PostgreSQL substitutes:
#   %p  full path to the WAL segment on disk
#   %f  WAL segment filename (no path)
#
# ALL credentials and options are read from container environment variables.
# The literal values never appear in postgresql.conf or pg_settings.
#
# Storj credentials — use ONE of the two methods:
#
# Method A — access grant (single value):
#   STORJ_ACCESS       — serialised Storj access grant
#
# Method B — API key + satellite + passphrase:
#   STORJ_API_KEY      — Storj API key
#   STORJ_SATELLITE    — satellite address, e.g. 121RTSDp...@ap1.storj.io:7777
#   STORJ_PASSPHRASE   — Storj encryption passphrase
#
# Required (both methods):
#   STORJ_BUCKET       — destination bucket (default: cloud-dump-wal-test)
#
# Optional:
#   WAL_PREFIX         — prefix inside the bucket          (default: wal_archive)
#   WAL_COMPRESS       — "false" to disable gzip           (default: true)
#   WAL_ENCRYPT        — "true" to enable encryption       (default: false)
#   WAL_PASSPHRASE     — encryption passphrase             (used when WAL_ENCRYPT=true)
#   WAL_RECIPIENT_KEY  — path to age public key file       (used when WAL_ENCRYPT=true)
#
# Usage in postgresql.conf:
#   archive_mode = on
#   archive_command = '/usr/local/bin/wal-archive %p %f'

set -e

WAL_PATH="$1"
WAL_NAME="$2"

if [ -z "$WAL_PATH" ] || [ -z "$WAL_NAME" ]; then
    echo "wal-archive: usage: wal-archive <wal-path> <wal-name>" >&2
    exit 1
fi

# ── Build argument list ───────────────────────────────────────────────────────

ARGS="wal-push $WAL_PATH $WAL_NAME"
ARGS="$ARGS --storage storj"

# Storj credentials: access grant takes priority over API key method.
if [ -n "${STORJ_ACCESS}" ]; then
    ARGS="$ARGS --storj-access ${STORJ_ACCESS}"
elif [ -n "${STORJ_API_KEY}" ] && [ -n "${STORJ_SATELLITE}" ] && [ -n "${STORJ_PASSPHRASE}" ]; then
    ARGS="$ARGS --storj-api-key ${STORJ_API_KEY}"
    ARGS="$ARGS --storj-satellite ${STORJ_SATELLITE}"
    ARGS="$ARGS --storj-passphrase ${STORJ_PASSPHRASE}"
else
    echo "wal-archive: no Storj credentials found. Set STORJ_ACCESS or STORJ_API_KEY+STORJ_SATELLITE+STORJ_PASSPHRASE" >&2
    exit 1
fi

ARGS="$ARGS --storj-bucket ${STORJ_BUCKET:-cloud-dump-wal-test}"

# Cluster-aware WAL prefix: if CLUSTER is set, use <cluster>/wal unless WAL_PREFIX overrides.
if [ -n "${CLUSTER}" ] && [ "${WAL_PREFIX:-}" = "" ]; then
    ARGS="$ARGS --cluster ${CLUSTER}"
else
    ARGS="$ARGS --wal-prefix ${WAL_PREFIX:-wal_archive}"
fi

# Compression (enabled by default; set WAL_COMPRESS=false to skip)
if [ "${WAL_COMPRESS:-true}" = "false" ]; then
    ARGS="$ARGS --compress=false"
fi

# Encryption
if [ "${WAL_ENCRYPT:-false}" = "true" ]; then
    ARGS="$ARGS --encrypt"
    if [ -n "${WAL_PASSPHRASE}" ]; then
        ARGS="$ARGS --passphrase ${WAL_PASSPHRASE}"
    fi
    if [ -n "${WAL_RECIPIENT_KEY}" ]; then
        ARGS="$ARGS --recipient-key ${WAL_RECIPIENT_KEY}"
    fi
fi

# shellcheck disable=SC2086
exec /usr/local/bin/cloud-dump $ARGS
