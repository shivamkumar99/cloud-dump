package pgbackup

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// BackupConfig holds all parameters for a backup run.
type BackupConfig struct {
	DBUrl      string // must include "replication=yes"
	BackupName string
	Parallel   int
	Encryptor  crypto.Encryptor
	Storage    storage.Storage
	Log        zerolog.Logger
}

// backupSession holds the state shared across the backup pipeline steps.
// Keeping state in a struct instead of passing 10+ variables between helpers
// makes the flow easy to follow without deep parameter lists.
type backupSession struct {
	cfg             BackupConfig
	log             zerolog.Logger
	conn            *pgconn.PgConn
	encrypted       bool
	pgVersion15Plus bool

	sysID     pglogrepl.IdentifySystemResult
	result    pglogrepl.BaseBackupResult
	startTime time.Time
}

// tsWork describes a single tablespace that must be streamed and uploaded.
type tsWork struct {
	oid      int32
	location string
	storKey  string
}

// Backup performs a physical base backup of the PostgreSQL cluster via the
// BASE_BACKUP streaming replication protocol — no pg_dump binary, no COPY,
// no disk writes. Each tablespace TAR is compressed, optionally encrypted,
// and streamed directly to storage.
//
// The DB connection reads tablespaces sequentially (protocol constraint).
// Upload goroutines run in parallel — io.Pipe decouples reading from uploading.
func Backup(ctx context.Context, cfg BackupConfig) (*Manifest, error) {
	s := &backupSession{
		cfg:       cfg,
		log:       cfg.Log.With().Str("backup", cfg.BackupName).Logger(),
		encrypted: !isNoop(cfg.Encryptor),
	}

	// ── 1. Connect ───────────────────────────────────────────────────────
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	defer s.conn.Close(ctx)

	// ── 2. Identify system + log timeline history ────────────────────────
	if err := s.identifySystem(ctx); err != nil {
		return nil, err
	}
	s.logTimelineHistory(ctx)

	// ── 3. Start BASE_BACKUP ─────────────────────────────────────────────
	if err := s.startBaseBackup(ctx); err != nil {
		return nil, err
	}

	// ── 4. Stream & upload each tablespace ───────────────────────────────
	tablespaces := s.buildTablespaceList()
	manifestTSList, err := s.streamTablespaces(ctx, tablespaces)
	if err != nil {
		return nil, err
	}

	// ── 5. Finish backup + write manifest ────────────────────────────────
	return s.finishAndWriteManifest(ctx, manifestTSList)
}

// connect opens the replication-mode connection to PostgreSQL.
func (s *backupSession) connect(ctx context.Context) error {
	s.log.Info().Str("db_url", s.cfg.DBUrl).Msg("connecting (replication mode)")

	conn, err := pgconn.Connect(ctx, s.cfg.DBUrl)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	s.conn = conn
	return nil
}

// identifySystem calls IDENTIFY_SYSTEM and detects the PG major version.
func (s *backupSession) identifySystem(ctx context.Context) error {
	sysID, err := pglogrepl.IdentifySystem(ctx, s.conn)
	if err != nil {
		return fmt.Errorf("IDENTIFY_SYSTEM: %w", err)
	}
	s.sysID = sysID
	s.pgVersion15Plus = pgMajorVersion(s.conn) >= 15

	s.log.Info().
		Str("system_id", sysID.SystemID).
		Int32("timeline", sysID.Timeline).
		Str("xlog_pos", sysID.XLogPos.String()).
		Bool("pg15_plus_framing", s.pgVersion15Plus).
		Msg("system identified")

	return nil
}

// logTimelineHistory logs the timeline history if the server has been
// promoted or recovered (timeline > 1). Non-fatal on failure.
func (s *backupSession) logTimelineHistory(ctx context.Context) {
	if s.sysID.Timeline <= 1 {
		s.log.Info().
			Int32("timeline", s.sysID.Timeline).
			Msg("timeline 1 — no prior history (server has never been promoted or recovered)")
		return
	}

	tlh, err := pglogrepl.TimelineHistory(ctx, s.conn, s.sysID.Timeline)
	if err != nil {
		s.log.Warn().Err(err).Int32("timeline", s.sysID.Timeline).Msg("could not fetch timeline history")
		return
	}
	s.log.Info().
		Str("filename", tlh.FileName).
		Str("content", string(tlh.Content)).
		Msg("timeline history")
}

// startBaseBackup issues START_BASE_BACKUP and records the start time.
func (s *backupSession) startBaseBackup(ctx context.Context) error {
	s.startTime = time.Now()

	opts := pglogrepl.BaseBackupOptions{
		Label:  s.cfg.BackupName,
		WAL:    true, // include pg_wal contents for self-contained restore
		Fast:   true, // fast checkpoint
		NoWait: true,
	}

	result, err := pglogrepl.StartBaseBackup(ctx, s.conn, opts)
	if err != nil {
		return fmt.Errorf("START_BASE_BACKUP: %w", err)
	}
	s.result = result

	s.log.Info().
		Str("start_lsn", result.LSN.String()).
		Int("extra_tablespaces", len(result.Tablespaces)).
		Msg("base backup started")

	return nil
}

// buildTablespaceList returns the ordered list of tablespaces to stream.
// The base PGDATA directory (OID 0) is always first.
func (s *backupSession) buildTablespaceList() []tsWork {
	allTS := make([]tsWork, 0, 1+len(s.result.Tablespaces))
	allTS = append(allTS, tsWork{
		oid:     0,
		storKey: tablespaceKey(s.cfg.BackupName, 0, s.encrypted),
	})
	for _, ts := range s.result.Tablespaces {
		allTS = append(allTS, tsWork{
			oid:      ts.OID,
			location: ts.Location,
			storKey:  tablespaceKey(s.cfg.BackupName, ts.OID, s.encrypted),
		})
	}
	return allTS
}

// streamTablespaces reads each tablespace TAR from the replication connection
// and uploads it to storage in parallel. Returns the manifest tablespace list.
func (s *backupSession) streamTablespaces(ctx context.Context, allTS []tsWork) ([]Tablespace, error) {
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	errCh := make(chan error, len(allTS))

	manifestTSList := make([]Tablespace, 0, len(allTS))

	for _, ts := range allTS {
		if err := pglogrepl.NextTableSpace(ctx, s.conn); err != nil {
			return nil, fmt.Errorf("advancing to tablespace OID=%d: %w", ts.oid, err)
		}

		tsLog := s.log.With().Int32("oid", ts.oid).Str("key", ts.storKey).Logger()
		tsLog.Info().Msg("streaming tablespace")

		pr, pw := io.Pipe()

		sem <- struct{}{} // acquire upload slot
		wg.Add(1)

		go func(r *io.PipeReader, key string, l zerolog.Logger) {
			defer wg.Done()
			defer func() { <-sem }()

			t0 := time.Now()
			if err := uploadStream(ctx, s.cfg.Storage, s.cfg.Encryptor, key, r, l); err != nil {
				errCh <- fmt.Errorf("upload %q: %w", key, err)
				_ = r.CloseWithError(err)
				return
			}
			l.Info().Str("elapsed", formatDuration(time.Since(t0))).Msg("tablespace uploaded")
		}(pr, ts.storKey, tsLog)

		writeErr := drainCopyData(ctx, s.conn, pw, s.pgVersion15Plus)
		pw.CloseWithError(writeErr)

		if writeErr != nil {
			wg.Wait()
			return nil, fmt.Errorf("reading tablespace OID=%d: %w", ts.oid, writeErr)
		}

		manifestTSList = append(manifestTSList, Tablespace{
			OID:      ts.oid,
			Location: ts.location,
			StorKey:  ts.storKey,
		})
	}

	wg.Wait()
	close(errCh)

	if err := collectErrors(errCh); err != nil {
		return nil, err
	}
	return manifestTSList, nil
}

// finishAndWriteManifest calls FINISH_BASE_BACKUP, builds the manifest,
// uploads it, and logs completion.
func (s *backupSession) finishAndWriteManifest(ctx context.Context, tablespaces []Tablespace) (*Manifest, error) {
	endResult, err := pglogrepl.FinishBaseBackup(ctx, s.conn)
	if err != nil {
		return nil, fmt.Errorf("FINISH_BASE_BACKUP: %w", err)
	}

	manifest := &Manifest{
		BackupName:      s.cfg.BackupName,
		Timestamp:       s.startTime.UTC(),
		PostgresVersion: s.conn.ParameterStatus("server_version"),
		SystemID:        s.sysID.SystemID,
		BackupLabel:     s.cfg.BackupName,
		StartLSN:        s.result.LSN.String(),
		EndLSN:          endResult.LSN.String(),
		Encrypted:       s.encrypted,
		Tablespaces:     tablespaces,
	}

	if err := WriteManifest(ctx, s.cfg.Storage, manifest); err != nil {
		return nil, err
	}

	s.log.Info().
		Str("start_lsn", manifest.StartLSN).
		Str("end_lsn", manifest.EndLSN).
		Int("tablespaces", len(manifest.Tablespaces)).
		Str("total_elapsed", formatDuration(time.Since(s.startTime))).
		Msg("backup complete")

	return manifest, nil
}

// collectErrors drains an error channel and returns a combined error if any.
func collectErrors(errCh <-chan error) error {
	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("upload errors: %v", errs)
	}
	return nil
}

// drainCopyData reads CopyData protocol messages from conn into pw until CopyDone.
// CopyDone is consumed here so the caller can call NextTableSpace immediately after.
//
// PostgreSQL 15+ uses a sub-message framing inside CopyData for BASE_BACKUP:
//
//	'n' — new archive header (contains filename; we skip it)
//	'd' — archive data (actual tar bytes start at Data[1:])
//	'p' — progress report (skip)
//	'm' — manifest header (skip — we write our own manifest)
//
// PostgreSQL < 15 sends raw tar bytes directly in CopyData.Data with no prefix.
func drainCopyData(ctx context.Context, conn *pgconn.PgConn, pw *io.PipeWriter, pgVersion15Plus bool) error {
	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receiving message: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}
			if pgVersion15Plus {
				// PG 15+ CopyData framing: first byte is a sub-message type.
				switch m.Data[0] {
				case 'd': // archive data
					if _, err := pw.Write(m.Data[1:]); err != nil {
						return err
					}
				case 'n', 'p', 'm':
					// new-archive header, progress, manifest header — skip
				default:
					// Unknown sub-type — skip to be forward-compatible.
				}
			} else {
				// PG < 15: raw tar bytes, no framing.
				if _, err := pw.Write(m.Data); err != nil {
					return err
				}
			}
		case *pgproto3.CopyDone:
			return nil
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(m)
		default:
			return fmt.Errorf("unexpected message %T in CopyData stream", msg)
		}
	}
}

// uploadStream compresses r with gzip, optionally encrypts, then uploads to storage.
// Pipeline: r → progressReader → gzip → [Encryptor] → Storage.Upload
//
// A background goroutine logs upload progress every 5 seconds so the user can
// see that the upload is still alive during large backups.
func uploadStream(
	ctx context.Context,
	st storage.Storage,
	enc crypto.Encryptor,
	key string,
	r io.Reader,
	log zerolog.Logger,
) error {
	pr, pw := io.Pipe()

	tracker := &progressTracker{}
	trackedR := io.TeeReader(r, tracker)

	t0 := time.Now()
	stopProgress := startProgressLogger(tracker, t0, log)

	uploadErr := startUploadConsumer(ctx, st, key, pr)

	writeErr := compressAndEncrypt(enc, pw, trackedR)
	pw.CloseWithError(writeErr)

	// Wait for the upload goroutine to finish.
	err := <-uploadErr
	close(stopProgress)

	logUploadSummary(tracker, t0, log)

	if writeErr != nil {
		return writeErr
	}
	return err
}

// startProgressLogger spawns a goroutine that logs upload progress every 5s.
// Close the returned channel to stop it.
func startProgressLogger(tracker *progressTracker, t0 time.Time, log zerolog.Logger) chan struct{} {
	stop := make(chan struct{})
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Info().
					Str("uploaded", formatBytes(tracker.Bytes())).
					Str("elapsed", formatDuration(time.Since(t0))).
					Str("speed", formatSpeed(tracker.Bytes(), time.Since(t0))).
					Msg("upload in progress")
			case <-stop:
				return
			}
		}
	}()
	return stop
}

// startUploadConsumer starts a goroutine that reads from pr and uploads to
// storage. Returns a channel that delivers the upload error (nil on success).
func startUploadConsumer(ctx context.Context, st storage.Storage, key string, pr *io.PipeReader) <-chan error {
	ch := make(chan error, 1)
	go func() {
		err := st.Upload(ctx, key, pr)
		if err != nil {
			_ = pr.CloseWithError(err)
		}
		ch <- err
	}()
	return ch
}

// compressAndEncrypt pipes trackedR through gzip → encryptor → pw.
// Closes gzip and encryptor writers; the caller must close pw.
func compressAndEncrypt(enc crypto.Encryptor, pw *io.PipeWriter, trackedR io.Reader) error {
	encW, err := enc.Encrypt(pw)
	if err != nil {
		return fmt.Errorf("initialising encryption: %w", err)
	}
	gz := gzip.NewWriter(encW)

	_, copyErr := io.Copy(gz, trackedR)

	if closeErr := gz.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if closeErr := encW.Close(); copyErr == nil {
		copyErr = closeErr
	}
	return copyErr
}

// logUploadSummary logs the final upload size, elapsed time, and speed.
func logUploadSummary(tracker *progressTracker, t0 time.Time, log zerolog.Logger) {
	totalBytes := tracker.Bytes()
	elapsed := time.Since(t0)
	log.Info().
		Str("size", formatBytes(totalBytes)).
		Str("elapsed", formatDuration(elapsed)).
		Str("speed", formatSpeed(totalBytes, elapsed)).
		Msg("upload finished")
}

// tablespaceKey builds the storage object key for a tablespace TAR.
func tablespaceKey(backupName string, oid int32, encrypted bool) string {
	var name string
	if oid == 0 {
		name = "base.tar.gz"
	} else {
		name = fmt.Sprintf("%d.tar.gz", oid)
	}
	if encrypted {
		name += ".age"
	}
	return backupName + "/" + name
}

// isNoop reports whether enc is the passthrough (no-op) encryptor.
func isNoop(enc crypto.Encryptor) bool {
	_, ok := enc.(crypto.NoopEncryptor)
	return ok
}

// pgMajorVersion extracts the major version from the server_version parameter.
// Returns 0 if the version cannot be parsed.
func pgMajorVersion(conn *pgconn.PgConn) int {
	ver := conn.ParameterStatus("server_version")
	dot := strings.IndexByte(ver, '.')
	if dot == -1 {
		// Could be a single-number version like "17"
		dot = len(ver)
	}
	n, err := strconv.Atoi(ver[:dot])
	if err != nil {
		return 0
	}
	return n
}

// ── Progress tracking & formatting helpers ───────────────────────────────────

// progressTracker is an io.Writer that counts bytes written to it (thread-safe).
type progressTracker struct {
	n int64
}

func (p *progressTracker) Write(b []byte) (int, error) {
	atomic.AddInt64(&p.n, int64(len(b)))
	return len(b), nil
}

func (p *progressTracker) Bytes() int64 {
	return atomic.LoadInt64(&p.n)
}

// formatBytes returns a human-readable byte size (e.g. "42.5 MB").
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// formatDuration formats a duration in a human-friendly way:
//
//	< 1s  → "0.3s"
//	< 60s → "12.4s"
//	< 1h  → "2m34s"
//	≥ 1h  → "1h05m12s"
func formatDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := d.Seconds() - float64(h*3600+m*60)

	switch {
	case h > 0:
		return fmt.Sprintf("%dh%02dm%02ds", h, m, int(s))
	case m > 0:
		return fmt.Sprintf("%dm%02ds", m, int(s))
	default:
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
}

// formatSpeed returns a human-readable transfer speed (e.g. "12.3 MB/s").
func formatSpeed(bytes int64, d time.Duration) string {
	if d <= 0 {
		return "0 B/s"
	}
	bps := float64(bytes) / d.Seconds()
	return formatBytes(int64(bps)) + "/s"
}
