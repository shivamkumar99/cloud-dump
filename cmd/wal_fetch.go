package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
)

var (
	walFetchPassphrase string
	walFetchIdentity   string
	walFetchPrefix     string
)

var walFetchCmd = &cobra.Command{
	Use:   "wal-fetch <wal-file-name> <destination-path>",
	Short: "Fetch a WAL file from cloud storage (used as restore_command)",
	Long: `Downloads a single PostgreSQL WAL segment from cloud storage.

Designed to be used as PostgreSQL's restore_command during recovery:

  restore_command = 'cloud-dump wal-fetch %f %p --storage storj --storj-access "<grant>" --storj-bucket my-backups'

PostgreSQL calls this command for each WAL segment it needs during recovery.

Exit code 0 means the segment was fetched — PostgreSQL replays it.
Non-zero means the segment was not found — PostgreSQL ends recovery and promotes.`,
	Example: `  # Typically set automatically by cloud-dump restore, but can be manual:
  restore_command = 'cloud-dump wal-fetch %f %p \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups'

  # With decryption:
  restore_command = 'cloud-dump wal-fetch %f %p --passphrase "secret" \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups'`,
	Args: cobra.ExactArgs(2),
	RunE: runWalFetch,
}

func init() {
	rootCmd.AddCommand(walFetchCmd)

	walFetchCmd.Flags().StringVar(&walFetchPassphrase, "passphrase", "", "Decryption passphrase (if WAL was encrypted with --passphrase)")
	walFetchCmd.Flags().StringVar(&walFetchIdentity, "identity-key", "", "Path to age private key file (if WAL was encrypted with --recipient-key)")
	walFetchCmd.Flags().StringVar(&walFetchPrefix, "wal-prefix", "wal_archive", "Storage prefix for WAL files")
}

func runWalFetch(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	walFileName := args[0]
	destPath := args[1]

	// ── Env var fallbacks ─────────────────────────────────────────────────
	// WAL_PASSPHRASE=...   → --passphrase
	// WAL_IDENTITY_KEY=... → --identity-key
	if walFetchPassphrase == "" {
		walFetchPassphrase = os.Getenv("WAL_PASSPHRASE")
	}
	if walFetchIdentity == "" {
		walFetchIdentity = os.Getenv("WAL_IDENTITY_KEY")
	}

	// ── Storage ───────────────────────────────────────────────────────────
	st, err := newStorage(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	// ── Encryptor ─────────────────────────────────────────────────────────
	// If passphrase or identity key is provided, we can decrypt.
	// Otherwise NoopEncryptor — WalFetch handles the mismatch detection.
	doEncrypt := walFetchPassphrase != "" || walFetchIdentity != ""
	enc, err := buildEncryptor(doEncrypt, walFetchPassphrase, "", walFetchIdentity)
	if err != nil {
		return err
	}

	// ── Fetch ─────────────────────────────────────────────────────────────
	cfg := pgbackup.WalFetchConfig{
		FileName:  walFileName,
		DestPath:  destPath,
		WalPrefix: clusterWALPrefix(walFetchPrefix, cmd.Flags().Changed("wal-prefix")),
		Encryptor: enc,
		Storage:   st,
		Log:       log.Logger,
	}

	if err := pgbackup.WalFetch(ctx, cfg); err != nil {
		if err == pgbackup.ErrWalNotFound {
			// Exit non-zero so PostgreSQL knows recovery is complete.
			log.Info().Str("wal_file", walFileName).Msg("WAL file not found — end of archive")
			os.Exit(1)
		}
		return fmt.Errorf("wal-fetch failed: %w", err)
	}

	return nil
}
