package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

var (
	// Shared flags populated by PersistentPreRunE.
	dbURL    string
	logLevel string
	parallel int
	cluster  string

	// Storage flags.
	storageProvider string
	storjAccess     string
	storjBucket     string
	storjAPIKey     string
	storjSatellite  string
	storjPassphrase string
)

var rootCmd = &cobra.Command{
	Use:   "cloud-dump",
	Short: "Stream complete PostgreSQL physical backups to cloud storage",
	Long: `cloud-dump backs up a PostgreSQL cluster using the streaming replication
protocol (no pg_dump required). Backups stream directly to cloud storage
with optional gzip compression and age encryption.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// ── Env var fallbacks for storage flags ───────────────────────────
		// CLI flag always wins. If not provided, check the environment.
		// Mapping:
		//   STORJ_ACCESS      → --storj-access
		//   STORJ_BUCKET      → --storj-bucket
		//   STORJ_API_KEY     → --storj-api-key
		//   STORJ_SATELLITE   → --storj-satellite
		//   STORJ_PASSPHRASE  → --storj-passphrase
		//   CLOUD_DUMP_DB_URL → --db-url
		if storjAccess == "" {
			storjAccess = os.Getenv("STORJ_ACCESS")
		}
		if storjBucket == "" {
			storjBucket = os.Getenv("STORJ_BUCKET")
		}
		if storjAPIKey == "" {
			storjAPIKey = os.Getenv("STORJ_API_KEY")
		}
		if storjSatellite == "" {
			storjSatellite = os.Getenv("STORJ_SATELLITE")
		}
		if storjPassphrase == "" {
			storjPassphrase = os.Getenv("STORJ_PASSPHRASE")
		}
		if dbURL == "" {
			dbURL = os.Getenv("CLOUD_DUMP_DB_URL")
		}
		if cluster == "" {
			cluster = os.Getenv("CLOUD_DUMP_CLUSTER")
		}

		// Configure zerolog.
		level, err := zerolog.ParseLevel(strings.ToLower(logLevel))
		if err != nil {
			return fmt.Errorf("invalid log level %q: %w", logLevel, err)
		}
		zerolog.SetGlobalLevel(level)

		// Human-readable output to stderr when attached to a terminal.
		if isTerminal() {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"})
		}
		return nil
	},
}

// Execute is the entry point called from main.go.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dbURL, "db-url", "", "PostgreSQL connection URL (must include replication=yes for backup)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().IntVar(&parallel, "parallel", 4, "Number of parallel upload/download goroutines")
	rootCmd.PersistentFlags().StringVar(&cluster, "cluster", "", "Cluster name — groups backups and WAL under a shared prefix (e.g. prod-pg17). Backups go to <cluster>/backups/<name>/, WAL to <cluster>/wal/")

	rootCmd.PersistentFlags().StringVar(&storageProvider, "storage", "storj", "Storage backend (storj)")
	rootCmd.PersistentFlags().StringVar(&storjAccess, "storj-access", "", "Storj serialised access grant")
	rootCmd.PersistentFlags().StringVar(&storjBucket, "storj-bucket", "", "Storj bucket name")
	rootCmd.PersistentFlags().StringVar(&storjAPIKey, "storj-api-key", "", "Storj API key (alternative to --storj-access)")
	rootCmd.PersistentFlags().StringVar(&storjSatellite, "storj-satellite", "", "Storj satellite address (alternative to --storj-access)")
	rootCmd.PersistentFlags().StringVar(&storjPassphrase, "storj-passphrase", "", "Storj encryption passphrase (alternative to --storj-access)")
}

// newStorage constructs a Storage from the shared flags.
func newStorage(ctx context.Context) (storage.Storage, error) {
	cfg := storage.Config{
		Provider:        storageProvider,
		StorjAccess:     storjAccess,
		StorjBucket:     storjBucket,
		StorjAPIKey:     storjAPIKey,
		StorjSatellite:  storjSatellite,
		StorjPassphrase: storjPassphrase,
	}
	st, err := storage.NewStorage(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("initialising storage: %w", err)
	}
	return st, nil
}

// clusterBackupKey returns the storage key for a named backup.
// With --cluster: "<cluster>/backup/<name>"
// Without:        "<name>"  (backward-compatible)
func clusterBackupKey(name string) string {
	if cluster == "" {
		return name
	}
	return cluster + "/backup/" + name
}

// clusterWALPrefix returns the WAL storage prefix.
// With --cluster: "<cluster>/wal_archive"
// Without:        the explicit walPrefix value passed in
func clusterWALPrefix(explicitPrefix string, flagChanged bool) string {
	if cluster != "" && !flagChanged {
		return cluster + "/wal_archive"
	}
	return explicitPrefix
}

// isTerminal returns true when os.Stderr is an interactive terminal.
func isTerminal() bool {
	fi, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
