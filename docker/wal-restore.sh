#!/bin/sh
# WAL restore wrapper — called by PostgreSQL as restore_command during recovery.
#
# PostgreSQL substitutes:
#   %f  WAL segment filename to fetch
#   %p  destination path where the segment should be written
#
# ALL credentials and options are read from container environment variables.
#
# Storj credentials — use ONE of the two methods:
#
# Method A — access grant (single value):
#   STORJ_ACCESS      — serialised Storj access grant
#
# Method B — API key + satellite + passphrase:
#   STORJ_API_KEY     — Storj API key
#   STORJ_SATELLITE   — satellite address, e.g. 121RTSDp...@ap1.storj.io:7777
#   STORJ_PASSPHRASE  — Storj encryption passphrase
#
# Required (both methods):
#   STORJ_BUCKET      — source bucket       (default: cloud-dump-wal-test)
#
# Optional:
#   WAL_PREFIX        — prefix inside the bucket    (default: wal_archive)
#   WAL_PASSPHRASE    — decryption passphrase        (if WAL was encrypted with WAL_PASSPHRASE)
#   WAL_IDENTITY_KEY  — path to age private key file (if WAL was encrypted with WAL_RECIPIENT_KEY)
#
# Exit 0  → segment fetched, PostgreSQL replays it.
# Exit 1  → segment not found, PostgreSQL ends recovery and promotes.
#
# Usage (set restore_command manually or via cloud-dump restore --restore-command):
#   restore_command = '/usr/local/bin/wal-restore %f %p'

WAL_NAME="$1"
DEST_PATH="$2"

if [ -z "$WAL_NAME" ] || [ -z "$DEST_PATH" ]; then
    echo "wal-restore: usage: wal-restore <wal-name> <dest-path>" >&2
    exit 1
fi

# ── Build argument list ───────────────────────────────────────────────────────

ARGS="wal-fetch $WAL_NAME $DEST_PATH"
ARGS="$ARGS --storage storj"

# Storj credentials: access grant takes priority over API key method.
if [ -n "${STORJ_ACCESS}" ]; then
    ARGS="$ARGS --storj-access ${STORJ_ACCESS}"
elif [ -n "${STORJ_API_KEY}" ] && [ -n "${STORJ_SATELLITE}" ] && [ -n "${STORJ_PASSPHRASE}" ]; then
    ARGS="$ARGS --storj-api-key ${STORJ_API_KEY}"
    ARGS="$ARGS --storj-satellite ${STORJ_SATELLITE}"
    ARGS="$ARGS --storj-passphrase ${STORJ_PASSPHRASE}"
else
    echo "wal-restore: no Storj credentials found. Set STORJ_ACCESS or STORJ_API_KEY+STORJ_SATELLITE+STORJ_PASSPHRASE" >&2
    exit 1
fi

ARGS="$ARGS --storj-bucket ${STORJ_BUCKET:-cloud-dump-wal-test}"

# Cluster-aware WAL prefix: if CLUSTER is set, use <cluster>/wal unless WAL_PREFIX overrides.
if [ -n "${CLUSTER}" ] && [ "${WAL_PREFIX:-}" = "" ]; then
    ARGS="$ARGS --cluster ${CLUSTER}"
else
    ARGS="$ARGS --wal-prefix ${WAL_PREFIX:-wal_archive}"
fi

# Decryption
if [ -n "${WAL_PASSPHRASE}" ]; then
    ARGS="$ARGS --passphrase ${WAL_PASSPHRASE}"
fi
if [ -n "${WAL_IDENTITY_KEY}" ]; then
    ARGS="$ARGS --identity-key ${WAL_IDENTITY_KEY}"
fi

# shellcheck disable=SC2086
exec /usr/local/bin/cloud-dump $ARGS
