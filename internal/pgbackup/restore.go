package pgbackup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// RestoreConfig holds all parameters for a restore run.
type RestoreConfig struct {
	BackupName string
	PGDataDir  string // target PGDATA directory (must exist and be empty)
	Parallel   int
	Encryptor  crypto.Encryptor
	Storage    storage.Storage
	Log        zerolog.Logger

	// ── PITR fields (all optional) ────────────────────────────────────────
	// When any of these are set, cloud-dump writes a restore_command into
	// postgresql.auto.conf so PostgreSQL can fetch archived WAL segments
	// via `cloud-dump wal-fetch` during recovery.

	RecoveryTargetTime string // e.g. "2026-03-07 14:30:00 UTC"
	RecoveryTargetLSN  string // e.g. "0/5200000"

	// RestoreCommand is the fully-formed restore_command string that
	// PostgreSQL will execute for each WAL segment during recovery.
	// Built by the cmd layer (includes storage credentials + wal-prefix).
	// Example: cloud-dump wal-fetch %f %p --storage storj --storj-access "..." --storj-bucket my-backups
	RestoreCommand string
}

// DownloadResult is returned by Download and consumed by Apply.
// Caller must defer os.RemoveAll(result.StagingDir) when done.
type DownloadResult struct {
	StagingDir string
	Manifest   *Manifest
}

// Restore is a convenience wrapper: Download then Apply.
//
// After this completes:
//  1. recovery.signal is present in PGDATA — PostgreSQL will enter recovery mode on start.
//  2. The user starts PostgreSQL normally; it replays WAL and promotes to primary.
func Restore(ctx context.Context, cfg RestoreConfig) error {
	result, err := Download(ctx, cfg)
	if err != nil {
		return err
	}
	defer os.RemoveAll(result.StagingDir)

	return Apply(ctx, cfg, result)
}

// Download fetches all tablespace archives for cfg.BackupName from cloud storage
// to a local staging directory. The download is parallelised up to cfg.Parallel.
//
// Caller must defer os.RemoveAll(result.StagingDir) when done with the result.
func Download(ctx context.Context, cfg RestoreConfig) (*DownloadResult, error) {
	log := cfg.Log.With().Str("backup", cfg.BackupName).Logger()

	log.Info().Msg("reading manifest")
	manifest, err := ReadManifest(ctx, cfg.Storage, cfg.BackupName)
	if err != nil {
		return nil, err
	}

	log.Info().
		Str("postgres_version", manifest.PostgresVersion).
		Str("start_lsn", manifest.StartLSN).
		Str("end_lsn", manifest.EndLSN).
		Bool("encrypted", manifest.Encrypted).
		Int("tablespaces", len(manifest.Tablespaces)).
		Msg("manifest loaded")

	stagingDir, err := os.MkdirTemp("", "cloud-dump-*")
	if err != nil {
		return nil, fmt.Errorf("creating staging directory: %w", err)
	}

	sem := make(chan struct{}, cfg.Parallel)
	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	for _, ts := range manifest.Tablespaces {
		ts := ts // capture
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			tsLog := log.With().Int32("oid", ts.OID).Str("key", ts.StorKey).Logger()
			localPath := stagingPath(stagingDir, ts.OID)
			tsLog.Info().Str("staging", localPath).Msg("downloading tablespace")
			t0 := time.Now()

			if err := downloadTablespace(ctx, cfg.Storage, ts.StorKey, localPath); err != nil {
				errCh <- fmt.Errorf("tablespace OID=%d: %w", ts.OID, err)
				return
			}
			tsLog.Info().Dur("elapsed", time.Since(t0)).Msg("tablespace downloaded")
		}()
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}
	if len(errs) > 0 {
		os.RemoveAll(stagingDir)
		return nil, fmt.Errorf("download errors: %v", errs)
	}

	return &DownloadResult{StagingDir: stagingDir, Manifest: manifest}, nil
}

// Apply extracts previously downloaded tablespace archives (from result.StagingDir)
// into cfg.PGDataDir and writes PostgreSQL recovery configuration.
//
// cfg.PGDataDir must exist and be empty. Apply is parallelised up to cfg.Parallel.
func Apply(ctx context.Context, cfg RestoreConfig, result *DownloadResult) error {
	log := cfg.Log.With().Str("backup", cfg.BackupName).Str("pgdata", cfg.PGDataDir).Logger()

	if err := validatePGDataDir(cfg.PGDataDir); err != nil {
		return err
	}

	sem := make(chan struct{}, cfg.Parallel)
	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	for _, ts := range result.Manifest.Tablespaces {
		ts := ts // capture
		targetDir := cfg.PGDataDir
		if ts.OID != 0 {
			// Non-default tablespace: restore to its original location.
			targetDir = ts.Location
		}

		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			tsLog := log.With().Int32("oid", ts.OID).Logger()
			localPath := stagingPath(result.StagingDir, ts.OID)
			tsLog.Info().Str("target", targetDir).Msg("applying tablespace")
			t0 := time.Now()

			if err := applyTablespace(cfg.Encryptor, localPath, targetDir, tsLog); err != nil {
				errCh <- fmt.Errorf("tablespace OID=%d: %w", ts.OID, err)
				return
			}
			tsLog.Info().Dur("elapsed", time.Since(t0)).Msg("tablespace applied")
		}()
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("apply errors: %v", errs)
	}

	if err := writeRecoveryConfig(cfg); err != nil {
		return err
	}

	log.Info().Msg("restore complete — start PostgreSQL to begin WAL recovery")
	log.Info().Msgf("  pg_ctl start -D %s", cfg.PGDataDir)

	if cfg.RecoveryTargetTime != "" {
		log.Info().Str("recovery_target_time", cfg.RecoveryTargetTime).Msg("PITR — PostgreSQL will replay WAL up to the target time")
	}
	if cfg.RecoveryTargetLSN != "" {
		log.Info().Str("recovery_target_lsn", cfg.RecoveryTargetLSN).Msg("PITR — PostgreSQL will replay WAL up to the target LSN")
	}

	return nil
}

// stagingPath returns the local file path for a tablespace in the staging directory.
func stagingPath(stagingDir string, oid int32) string {
	return filepath.Join(stagingDir, fmt.Sprintf("ts_%d.blob", oid))
}

// downloadTablespace downloads a raw archive (encrypted+compressed) from storage to a local file.
func downloadTablespace(ctx context.Context, st storage.Storage, key, localPath string) error {
	rc, err := st.Download(ctx, key)
	if err != nil {
		return fmt.Errorf("downloading %q: %w", key, err)
	}
	defer rc.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("creating staging file %q: %w", localPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("writing staging file %q: %w", localPath, err)
	}
	return nil
}

// applyTablespace decrypts, decompresses, and extracts a staged archive to targetDir.
// Pipeline: local file → [decrypt] → gzip → tar.Extract
func applyTablespace(enc crypto.Encryptor, localPath, targetDir string, log zerolog.Logger) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening staged file %q: %w", localPath, err)
	}
	defer f.Close()

	decR, err := enc.Decrypt(f)
	if err != nil {
		return fmt.Errorf("decrypting %q: %w", localPath, err)
	}

	gz, err := gzip.NewReader(decR)
	if err != nil {
		return fmt.Errorf("creating gzip reader for %q: %w", localPath, err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar from %q: %w", localPath, err)
		}
		if err := extractEntry(tr, hdr, targetDir, log); err != nil {
			return err
		}
	}
	return nil
}

// extractEntry writes a single tar entry (file or directory) to targetDir.
func extractEntry(tr *tar.Reader, hdr *tar.Header, targetDir string, log zerolog.Logger) error {
	// Guard against path traversal attacks.
	target := filepath.Join(targetDir, filepath.Clean("/"+hdr.Name))
	if !strings.HasPrefix(target, filepath.Clean(targetDir)+string(filepath.Separator)) {
		return fmt.Errorf("tar entry %q escapes target directory — aborting", hdr.Name)
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
			return fmt.Errorf("creating directory %q: %w", target, err)
		}

	case tar.TypeReg, tar.TypeRegA:
		if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
			return fmt.Errorf("creating parent of %q: %w", target, err)
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
		if err != nil {
			return fmt.Errorf("creating file %q: %w", target, err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return fmt.Errorf("writing file %q: %w", target, err)
		}
		f.Close()

	case tar.TypeSymlink:
		if err := os.Symlink(hdr.Linkname, target); err != nil && !os.IsExist(err) {
			return fmt.Errorf("creating symlink %q → %q: %w", target, hdr.Linkname, err)
		}

	default:
		log.Debug().Str("file", hdr.Name).Int("type", int(hdr.Typeflag)).Msg("skipping unsupported tar entry type")
	}

	return nil
}

// writeRecoveryConfig writes PostgreSQL recovery configuration after restore.
//
// Plain restore (no PITR): no recovery.signal is written. The backup_label
// file in the archive tells PostgreSQL to enter crash recovery automatically,
// and the WAL segments in pg_wal/ are sufficient to reach consistency.
//
// PITR restore (RestoreCommand set): writes recovery.signal so PostgreSQL
// enters targeted recovery, plus restore_command and optional target params
// in postgresql.auto.conf so it can fetch archived WAL segments.
func writeRecoveryConfig(cfg RestoreConfig) error {
	pgdata := cfg.PGDataDir
	pitr := cfg.RestoreCommand != "" || cfg.RecoveryTargetTime != "" || cfg.RecoveryTargetLSN != ""

	if !pitr {
		// Plain restore — backup_label triggers crash recovery automatically.
		// No recovery.signal needed (and writing it without restore_command
		// causes "must specify restore_command" FATAL on PG 12+).
		return nil
	}

	// recovery.signal — tells PostgreSQL to enter targeted recovery mode.
	signalPath := filepath.Join(pgdata, "recovery.signal")
	if err := os.WriteFile(signalPath, []byte{}, 0600); err != nil {
		return fmt.Errorf("writing recovery.signal: %w", err)
	}

	// Append recovery settings to postgresql.auto.conf.
	autoConfPath := filepath.Join(pgdata, "postgresql.auto.conf")
	f, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("opening postgresql.auto.conf: %w", err)
	}
	defer f.Close()

	var conf strings.Builder
	conf.WriteString("\n# Added by cloud-dump restore\n")

	if cfg.RestoreCommand != "" {
		fmt.Fprintf(&conf, "restore_command = '%s'\n", cfg.RestoreCommand)
	}
	if cfg.RecoveryTargetTime != "" {
		fmt.Fprintf(&conf, "recovery_target_time = '%s'\n", cfg.RecoveryTargetTime)
	}
	if cfg.RecoveryTargetLSN != "" {
		fmt.Fprintf(&conf, "recovery_target_lsn = '%s'\n", cfg.RecoveryTargetLSN)
	}
	conf.WriteString("recovery_target_action = 'promote'\n")

	if _, err := f.WriteString(conf.String()); err != nil {
		return fmt.Errorf("writing postgresql.auto.conf: %w", err)
	}
	return nil
}

// validatePGDataDir ensures the target directory exists and is empty.
func validatePGDataDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return fmt.Errorf("PGDATA directory %q does not exist — create it first", dir)
	}
	if err != nil {
		return fmt.Errorf("reading PGDATA directory: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("PGDATA directory %q is not empty — restore requires an empty directory", dir)
	}
	return nil
}
