#!/bin/bash
# Runs after PostgreSQL starts during container initialization.
# Creates the replication user and allows replication connections.
set -e

echo "==> Creating replication user and configuring pg_hba.conf..."

# Allow replication connections from any host inside the Docker network.
echo "host    replication     repl_user       all             md5" >> "$PGDATA/pg_hba.conf"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER repl_user WITH REPLICATION ENCRYPTED PASSWORD 'repl_password';
    SELECT pg_reload_conf();
EOSQL

echo "==> Replication user created."
