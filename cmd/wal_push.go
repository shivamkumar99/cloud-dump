package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
)

var (
	walCompress   bool
	walPrefix     string
	walEncrypt    bool
	walPassphrase string
	walRecipient  string
)

var walPushCmd = &cobra.Command{
	Use:   "wal-push <wal-file-path> <wal-file-name>",
	Short: "Archive a WAL file to cloud storage (used as archive_command)",
	Long: `Uploads a single PostgreSQL WAL segment to cloud storage.

Designed to be used as PostgreSQL's archive_command:

  archive_command = 'cloud-dump wal-push %p %f --storage storj --storj-access "<grant>" --storj-bucket my-backups'

PostgreSQL calls this command each time a 16 MB WAL segment is complete.
The command is idempotent — if the WAL file already exists in storage,
the upload is skipped.

Exit code 0 tells PostgreSQL the segment was archived successfully.
Non-zero tells PostgreSQL to retry.`,
	Example: `  # In postgresql.conf:
  archive_command = 'cloud-dump wal-push %p %f \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups'

  # With compression disabled:
  archive_command = 'cloud-dump wal-push %p %f --no-compress \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups'

  # With encryption:
  archive_command = 'cloud-dump wal-push %p %f --encrypt --passphrase "secret" \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups'`,
	Args: cobra.ExactArgs(2),
	RunE: runWalPush,
}

func init() {
	rootCmd.AddCommand(walPushCmd)

	walPushCmd.Flags().BoolVar(&walCompress, "compress", true, "Compress WAL file with gzip before upload")
	walPushCmd.Flags().StringVar(&walPrefix, "wal-prefix", "wal_archive", "Storage prefix for WAL files")
	walPushCmd.Flags().BoolVar(&walEncrypt, "encrypt", false, "Encrypt WAL file")
	walPushCmd.Flags().StringVar(&walPassphrase, "passphrase", "", "Encryption passphrase (used with --encrypt)")
	walPushCmd.Flags().StringVar(&walRecipient, "recipient-key", "", "Path to age public key file (used with --encrypt)")
}

func runWalPush(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	walFilePath := args[0]
	walFileName := args[1]

	// ── Env var fallbacks ─────────────────────────────────────────────────
	// WAL_ENCRYPT=true      → --encrypt
	// WAL_COMPRESS=false    → --compress=false
	// WAL_PASSPHRASE=...    → --passphrase
	// WAL_RECIPIENT_KEY=... → --recipient-key
	if !walEncrypt {
		walEncrypt = os.Getenv("WAL_ENCRYPT") == "true"
	}
	if walCompress {
		walCompress = os.Getenv("WAL_COMPRESS") != "false"
	}
	if walPassphrase == "" {
		walPassphrase = os.Getenv("WAL_PASSPHRASE")
	}
	if walRecipient == "" {
		walRecipient = os.Getenv("WAL_RECIPIENT_KEY")
	}

	// ── Storage ───────────────────────────────────────────────────────────
	st, err := newStorage(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	// ── Encryptor ─────────────────────────────────────────────────────────
	enc, err := buildEncryptor(walEncrypt, walPassphrase, walRecipient, "")
	if err != nil {
		return err
	}

	// ── Push ──────────────────────────────────────────────────────────────
	cfg := pgbackup.WalPushConfig{
		FilePath:  walFilePath,
		FileName:  walFileName,
		WalPrefix: clusterWALPrefix(walPrefix, cmd.Flags().Changed("wal-prefix")),
		Compress:  walCompress,
		Encryptor: enc,
		Storage:   st,
		Log:       log.Logger,
	}

	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		return fmt.Errorf("wal-push failed: %w", err)
	}

	return nil
}
