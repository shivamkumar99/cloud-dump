package cmd

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
)

var (
	restoreName        string
	pgDataDir          string
	identityKey        string
	recoveryTargetTime string
	recoveryTargetLSN  string
	restoreWalPrefix   string
	restoreWalPass     string
	restoreWalIdentity string
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a PostgreSQL cluster from a backup",
	Long: `Downloads and extracts a backup to the target PGDATA directory.
The directory must exist and be empty. After restore completes, start
PostgreSQL normally — it will replay WAL and promote to primary.

For Point-in-Time Recovery (PITR), use --recovery-target-time or
--recovery-target-lsn. This requires a WAL archive created by
'cloud-dump wal-push' (via PostgreSQL's archive_command). PostgreSQL
will fetch WAL segments using 'cloud-dump wal-fetch' during recovery
and replay them up to the specified target.`,
	Example: `  # Standard restore (to backup time)
  cloud-dump restore \
    --name prod-2026-03-07 \
    --pgdata /var/lib/postgresql/17/main \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups

  # Point-in-Time Recovery (restore to a specific timestamp)
  cloud-dump restore \
    --name prod-2026-03-07 \
    --pgdata /var/lib/postgresql/17/main \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups \
    --recovery-target-time "2026-03-07 14:30:00 UTC"

  # PITR with encrypted WAL archive
  cloud-dump restore \
    --name prod-2026-03-07 \
    --pgdata /var/lib/postgresql/17/main \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups \
    --recovery-target-time "2026-03-07 14:30:00 UTC" \
    --wal-passphrase "my-secret"`,
	RunE: runRestore,
}

func init() {
	rootCmd.AddCommand(restoreCmd)

	restoreCmd.Flags().StringVar(&restoreName, "name", "", "Backup name to restore (required)")
	restoreCmd.Flags().StringVar(&pgDataDir, "pgdata", "", "Target PGDATA directory — must be empty (required)")
	restoreCmd.Flags().StringVar(&passphrase, "passphrase", "", "Decryption passphrase (if backup was encrypted with --passphrase)")
	restoreCmd.Flags().StringVar(&identityKey, "identity-key", "", "Path to age private key file (if backup was encrypted with --recipient-key)")

	// PITR flags
	restoreCmd.Flags().StringVar(&recoveryTargetTime, "recovery-target-time", "", "PITR: recover to this timestamp (e.g. \"2026-03-07 14:30:00 UTC\")")
	restoreCmd.Flags().StringVar(&recoveryTargetLSN, "recovery-target-lsn", "", "PITR: recover to this LSN (e.g. \"0/5200000\")")
	restoreCmd.Flags().StringVar(&restoreWalPrefix, "wal-prefix", "wal_archive", "Storage prefix for archived WAL files")
	restoreCmd.Flags().StringVar(&restoreWalPass, "wal-passphrase", "", "Decryption passphrase for encrypted WAL files")
	restoreCmd.Flags().StringVar(&restoreWalIdentity, "wal-identity-key", "", "Path to age private key for encrypted WAL files")

	_ = restoreCmd.MarkFlagRequired("name")
	_ = restoreCmd.MarkFlagRequired("pgdata")
}

func runRestore(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	// ── Storage ───────────────────────────────────────────────────────────
	st, err := newStorage(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	// ── Encryptor ─────────────────────────────────────────────────────────
	// Read manifest first to know if backup is encrypted.
	manifest, err := pgbackup.ReadManifest(ctx, st, restoreName)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}

	enc, err := buildEncryptor(manifest.Encrypted, passphrase, "", identityKey)
	if err != nil {
		return err
	}

	// ── Resolve WAL prefix ────────────────────────────────────────────────
	// With --cluster: auto-derive as <cluster>/wal (unless --wal-prefix was set explicitly).
	// Without --cluster: use --wal-prefix as-is.
	effectiveWALPrefix := clusterWALPrefix(restoreWalPrefix, cmd.Flags().Changed("wal-prefix"))

	// ── Build restore_command for PITR ────────────────────────────────────
	pitrRequested := recoveryTargetTime != "" || recoveryTargetLSN != ""
	var restoreCommand string

	if pitrRequested {
		restoreCommand, err = buildRestoreCommand(effectiveWALPrefix)
		if err != nil {
			return err
		}
		log.Info().
			Str("recovery_target_time", recoveryTargetTime).
			Str("recovery_target_lsn", recoveryTargetLSN).
			Str("wal_prefix", effectiveWALPrefix).
			Msg("PITR requested — restore_command will be written to postgresql.auto.conf")
	}

	// ── Run restore ───────────────────────────────────────────────────────
	cfg := pgbackup.RestoreConfig{
		BackupName:         clusterBackupKey(restoreName),
		PGDataDir:          pgDataDir,
		Parallel:           parallel,
		Encryptor:          enc,
		Storage:            st,
		Log:                log.Logger,
		RecoveryTargetTime: recoveryTargetTime,
		RecoveryTargetLSN:  recoveryTargetLSN,
		RestoreCommand:     restoreCommand,
	}

	if err := pgbackup.Restore(ctx, cfg); err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

	fmt.Printf("\nRestore complete. Start PostgreSQL to begin WAL recovery:\n")
	fmt.Printf("  pg_ctl start -D %s\n\n", pgDataDir)

	if pitrRequested {
		fmt.Printf("PITR enabled — PostgreSQL will replay archived WAL segments.\n")
		if recoveryTargetTime != "" {
			fmt.Printf("  recovery_target_time = '%s'\n", recoveryTargetTime)
		}
		if recoveryTargetLSN != "" {
			fmt.Printf("  recovery_target_lsn  = '%s'\n", recoveryTargetLSN)
		}
		fmt.Println()
	}

	return nil
}

// buildRestoreCommand constructs the restore_command string that PostgreSQL
// will execute for each WAL segment during PITR recovery. It embeds the
// storage credentials so `cloud-dump wal-fetch` can access the WAL archive.
func buildRestoreCommand(walPrefix string) (string, error) {
	parts := []string{"cloud-dump wal-fetch %f %p"}

	// Storage flags.
	parts = append(parts, fmt.Sprintf("--storage %s", storageProvider))

	switch {
	case storjAccess != "":
		parts = append(parts, fmt.Sprintf("--storj-access '%s'", storjAccess))
	case storjAPIKey != "" && storjSatellite != "" && storjPassphrase != "":
		parts = append(parts, fmt.Sprintf("--storj-api-key '%s'", storjAPIKey))
		parts = append(parts, fmt.Sprintf("--storj-satellite '%s'", storjSatellite))
		parts = append(parts, fmt.Sprintf("--storj-passphrase '%s'", storjPassphrase))
	default:
		return "", fmt.Errorf("PITR requires Storj credentials (--storj-access or --storj-api-key + --storj-satellite + --storj-passphrase) to build restore_command")
	}

	parts = append(parts, fmt.Sprintf("--storj-bucket %s", storjBucket))
	parts = append(parts, fmt.Sprintf("--wal-prefix %s", walPrefix))

	// WAL decryption credentials.
	if restoreWalPass != "" {
		parts = append(parts, fmt.Sprintf("--passphrase '%s'", restoreWalPass))
	}
	if restoreWalIdentity != "" {
		parts = append(parts, fmt.Sprintf("--identity-key %s", restoreWalIdentity))
	}

	return strings.Join(parts, " "), nil
}
