package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List available backups in storage",
	Example: `  cloud-dump list \
    --storage storj --storj-access "<grant>" --storj-bucket my-backups`,
	RunE: runList,
}

func init() {
	rootCmd.AddCommand(listCmd)
}

func runList(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	st, err := newStorage(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	// When --cluster is set, list only backups under <cluster>/backup/.
	// Without --cluster, list everything (backward-compatible).
	searchPrefix := ""
	stripPrefix := ""
	if cluster != "" {
		searchPrefix = cluster + "/backup/"
		stripPrefix = searchPrefix
	}

	backups, err := discoverBackups(ctx, st, searchPrefix, stripPrefix)
	if err != nil {
		return err
	}

	if len(backups) == 0 {
		if cluster != "" {
			fmt.Printf("No backups found under cluster %q.\n", cluster)
		} else {
			fmt.Println("No backups found.")
		}
		return nil
	}

	if cluster != "" {
		fmt.Printf("Cluster: %s  (WAL archive: %s/wal_archive/)\n\n", cluster, cluster)
	}
	fmt.Printf("%-40s  %-25s  %-10s  %s\n", "NAME", "TIMESTAMP", "ENCRYPTED", "PG VERSION")
	fmt.Println(strings.Repeat("-", 100))

	for _, m := range backups {
		enc := "no"
		if m.Encrypted {
			enc = "yes"
		}
		fmt.Printf("%-40s  %-25s  %-10s  %s\n",
			m.DisplayName,
			m.Timestamp.Format("2006-01-02 15:04:05 UTC"),
			enc,
			m.PostgresVersion,
		)
	}

	return nil
}

type backupEntry struct {
	*pgbackup.Manifest
	DisplayName string // short name shown to user (strip cluster prefix)
}

// discoverBackups lists all objects with a manifest.json suffix under searchPrefix
// and reads each manifest. stripPrefix is removed from the backup name for display.
func discoverBackups(ctx context.Context, st storage.Storage, searchPrefix, stripPrefix string) ([]*backupEntry, error) {
	keys, err := st.List(ctx, searchPrefix)
	if err != nil {
		return nil, fmt.Errorf("listing storage: %w", err)
	}

	var entries []*backupEntry
	seen := make(map[string]bool)

	for _, key := range keys {
		if !strings.HasSuffix(key, "/manifest.json") {
			continue
		}
		// Full storage key prefix for this backup (e.g. "prod-pg17/backups/2026-03-22").
		fullKey := strings.TrimSuffix(key, "/manifest.json")
		if seen[fullKey] {
			continue
		}
		seen[fullKey] = true

		m, err := pgbackup.ReadManifest(ctx, st, fullKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not read manifest for %q: %v\n", fullKey, err)
			continue
		}

		// Display name strips the cluster prefix so users pass --name without it.
		displayName := strings.TrimPrefix(fullKey, stripPrefix)
		entries = append(entries, &backupEntry{Manifest: m, DisplayName: displayName})
	}

	return entries, nil
}
