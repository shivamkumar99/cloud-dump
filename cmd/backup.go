package cmd

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
)

var (
	backupName    string
	encrypt       bool
	passphrase    string
	recipientKey  string
)

var backupCmd = &cobra.Command{ //nolint:gosec // G101: example URL in CLI help text — not real credentials
	Use:   "backup",
	Short: "Take a physical backup of the PostgreSQL cluster",
	Long: `Connects using the streaming replication protocol and streams the full
cluster backup (all databases, roles, permissions, sequences) to cloud storage.
No pg_dump binary required. No temp files written to disk.`,
	Example: `  # Unencrypted backup
  cloud-dump backup \
    --db-url "postgres://repl_user:pass@localhost/?replication=yes" \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups \
    --name prod-2026-03-07

  # Passphrase-encrypted backup
  cloud-dump backup ... --encrypt --passphrase "my-secret"

  # Public key encrypted backup
  cloud-dump backup ... --encrypt --recipient-key ~/.config/cloud-dump/age.key.pub`,
	RunE: runBackup,
}

func init() {
	rootCmd.AddCommand(backupCmd)

	backupCmd.Flags().StringVar(&backupName, "name", "", "Backup name / identifier (required)")
	backupCmd.Flags().BoolVar(&encrypt, "encrypt", false, "Enable encryption")
	backupCmd.Flags().StringVar(&passphrase, "passphrase", "", "Encryption passphrase (used with --encrypt)")
	backupCmd.Flags().StringVar(&recipientKey, "recipient-key", "", "Path to age public key file (used with --encrypt)")

	_ = backupCmd.MarkFlagRequired("name")
}

func runBackup(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	// ── Storage ───────────────────────────────────────────────────────────
	st, err := newStorage(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	// ── Encryptor ─────────────────────────────────────────────────────────
	enc, err := buildEncryptor(encrypt, passphrase, recipientKey, "")
	if err != nil {
		return err
	}

	// ── Run backup ────────────────────────────────────────────────────────
	cfg := pgbackup.BackupConfig{
		DBUrl:      dbURL,
		BackupName: clusterBackupKey(backupName),
		Parallel:   parallel,
		Encryptor:  enc,
		Storage:    st,
		Log:        log.Logger,
	}

	manifest, err := pgbackup.Backup(ctx, cfg)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	log.Info().
		Str("backup", manifest.BackupName).
		Str("start_lsn", manifest.StartLSN).
		Str("end_lsn", manifest.EndLSN).
		Bool("encrypted", manifest.Encrypted).
		Msgf("backup %q stored successfully", backupName)

	return nil
}

// buildEncryptor returns the correct Encryptor based on CLI flags.
// Dependency inversion: cmd layer picks the implementation; pgbackup never imports age.
func buildEncryptor(doEncrypt bool, pass, recipKey, identKey string) (crypto.Encryptor, error) {
	if !doEncrypt {
		return crypto.NoopEncryptor{}, nil
	}

	if pass != "" && recipKey != "" {
		return nil, fmt.Errorf("--passphrase and --recipient-key are mutually exclusive")
	}

	if pass != "" {
		return crypto.NewPassphraseEncryptor(pass), nil
	}

	if recipKey != "" {
		return crypto.NewKeyPairEncryptor(recipKey, identKey), nil
	}

	return nil, fmt.Errorf("--encrypt requires either --passphrase or --recipient-key")
}
