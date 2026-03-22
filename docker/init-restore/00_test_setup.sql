-- =============================================================================
-- postgres-restore initialisation (runs only on a fresh / pre-restore start)
--
-- PURPOSE: Seed the restore-target instance with a known state so you can
-- verify that cloud-dump restore actually overwrote everything.
--
-- After a successful restore from the source postgres you should see:
--   • repl_user, not test_user
--   • ecommerce_db, analytics_db, inventory_db  — not restore_marker
--   • ~85 MB of sample data, not this tiny marker table
-- =============================================================================

-- test_user — connect to postgres-restore before/after a restore to compare.
CREATE USER test_user WITH ENCRYPTED PASSWORD 'test_password';
GRANT ALL PRIVILEGES ON DATABASE postgres TO test_user;

-- Marker table — proves this is the PRE-RESTORE state.
-- If you run a restore and reconnect, this table will be gone.
CREATE TABLE restore_marker (
    id      SERIAL PRIMARY KEY,
    state   TEXT    NOT NULL,
    note    TEXT
);

INSERT INTO restore_marker (state, note) VALUES
    ('pre-restore',
     'This row exists only on a fresh postgres-restore instance. '
     'If you see it after running cloud-dump restore, the restore did not overwrite this instance.');
