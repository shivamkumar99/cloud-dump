package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"storj.io/uplink"
)

type storjStorage struct {
	project *uplink.Project
	bucket  string
}

func newStorjStorage(ctx context.Context, cfg Config) (*storjStorage, error) {
	var access *uplink.Access
	var err error

	switch {
	case cfg.StorjAccess != "":
		// Option 1: pre-serialized access grant.
		access, err = uplink.ParseAccess(cfg.StorjAccess)
		if err != nil {
			return nil, fmt.Errorf("parsing storj access grant: %w", err)
		}
	case cfg.StorjAPIKey != "" && cfg.StorjSatellite != "" && cfg.StorjPassphrase != "":
		// Option 2: derive access from API key + satellite + encryption passphrase.
		access, err = uplink.RequestAccessWithPassphrase(ctx, cfg.StorjSatellite, cfg.StorjAPIKey, cfg.StorjPassphrase)
		if err != nil {
			return nil, fmt.Errorf("requesting storj access: %w", err)
		}
	default:
		return nil, fmt.Errorf("storj requires either --storj-access OR all three of --storj-api-key, --storj-satellite, and --storj-passphrase")
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, fmt.Errorf("opening storj project: %w", err)
	}

	// Ensure the bucket exists (idempotent).
	_, err = project.EnsureBucket(ctx, cfg.StorjBucket)
	if err != nil {
		_ = project.Close()
		return nil, fmt.Errorf("ensuring storj bucket %q: %w", cfg.StorjBucket, err)
	}

	return &storjStorage{project: project, bucket: cfg.StorjBucket}, nil
}

func (s *storjStorage) Upload(ctx context.Context, key string, r io.Reader) error {
	upload, err := s.project.UploadObject(ctx, s.bucket, key, nil)
	if err != nil {
		return fmt.Errorf("starting upload for %q: %w", key, err)
	}

	if _, err = io.Copy(upload, r); err != nil {
		// Abort on copy failure so the incomplete object is discarded.
		_ = upload.Abort()
		return fmt.Errorf("uploading %q: %w", key, err)
	}

	if err = upload.Commit(); err != nil {
		return fmt.Errorf("committing upload for %q: %w", key, err)
	}
	return nil
}

func (s *storjStorage) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	dl, err := s.project.DownloadObject(ctx, s.bucket, key, nil)
	if err != nil {
		return nil, fmt.Errorf("starting download for %q: %w", key, err)
	}
	return dl, nil
}

func (s *storjStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.project.StatObject(ctx, s.bucket, key)
	if err != nil {
		if strings.Contains(err.Error(), "object not found") {
			return false, nil
		}
		return false, fmt.Errorf("stat %q: %w", key, err)
	}
	return true, nil
}

func (s *storjStorage) List(ctx context.Context, prefix string) ([]string, error) {
	// Ensure prefix ends with "/" so we list objects under a directory-like prefix.
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	iter := s.project.ListObjects(ctx, s.bucket, &uplink.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	var keys []string
	for iter.Next() {
		keys = append(keys, iter.Item().Key)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("listing objects with prefix %q: %w", prefix, err)
	}
	return keys, nil
}

func (s *storjStorage) Close() error {
	return s.project.Close()
}
