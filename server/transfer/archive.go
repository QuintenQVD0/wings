package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"

	"github.com/apex/log"
	"github.com/pelican-dev/wings/config"
	"github.com/pelican-dev/wings/internal/progress"
	"github.com/pelican-dev/wings/server/filesystem"
)

// Archive returns an archive that can be used to stream the contents of the
// contents of a server.
func (t *Transfer) Archive() (*Archive, error) {
	if t.archive == nil {
		// Get the disk usage of the server (used to calculate the progress of the archive process)
		rawSize, err := t.Server.Filesystem().DiskUsage(true)
		if err != nil {
			return nil, fmt.Errorf("transfer: failed to get server disk usage: %w", err)
		}

		// Create a new archive instance and assign it to the transfer.
		t.archive = NewArchive(t, uint64(rawSize))
	}

	return t.archive, nil
}

func (a *Archive) StreamBackups(ctx context.Context, mp *multipart.Writer) error {
	cfg := config.Get()
	backupPath := filepath.Join(cfg.System.BackupDirectory, a.transfer.Server.ID())

	// Check if backup directory exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		a.transfer.Log().Debug("no backups found to transfer")
		return nil
	}

	entries, err := os.ReadDir(backupPath)
	if err != nil {
		return err
	}

	backupCount := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tar.gz") {
			backupCount++
			backupFile := filepath.Join(backupPath, entry.Name())

			a.transfer.Log().WithField("backup", entry.Name()).Debug("streaming backup file")

			// Open backup file for reading
			file, err := os.Open(backupFile)
			if err != nil {
				return fmt.Errorf("failed to open backup file %s: %w", backupFile, err)
			}

			// Create hasher for this specific backup
			backupHasher := sha256.New()
			backupTee := io.TeeReader(file, backupHasher)

			// Create form file for the backup
			part, err := mp.CreateFormFile("backup_"+entry.Name(), entry.Name())
			if err != nil {
				file.Close()
				return fmt.Errorf("failed to create form file for backup %s: %w", entry.Name(), err)
			}

			// Stream the backup file
			if _, err := io.Copy(part, backupTee); err != nil {
				file.Close()
				return fmt.Errorf("failed to stream backup file %s: %w", entry.Name(), err)
			}
			file.Close()

			// Write individual backup checksum
			checksumField := "checksum_backup_" + entry.Name()
			if err := mp.WriteField(checksumField, hex.EncodeToString(backupHasher.Sum(nil))); err != nil {
				return fmt.Errorf("failed to write checksum for backup %s: %w", entry.Name(), err)
			}

			// Update progress for this backup file - use the correct method
			info, err := entry.Info()
			if err == nil {
				a.archive.Progress.SetTotal(a.archive.Progress.Written() + uint64(info.Size()))
			}

			a.transfer.Log().WithFields(log.Fields{
				"backup":   entry.Name(),
				"checksum": checksumField,
			}).Debug("backup file streamed with checksum")
		}
	}

	a.transfer.Log().WithField("count", backupCount).Debug("finished streaming backups")
	return nil
}

// Archive represents an archive used to transfer the contents of a server.
type Archive struct {
	archive  *filesystem.Archive
	transfer *Transfer
}

// NewArchive returns a new archive associated with the given transfer.
func NewArchive(t *Transfer, size uint64) *Archive {
	return &Archive{
		archive: &filesystem.Archive{
			Filesystem: t.Server.Filesystem(),
			Progress:   progress.NewProgress(size),
		},
		transfer: t,
	}
}

// Stream returns a reader that can be used to stream the contents of the archive.
func (a *Archive) Stream(ctx context.Context, w io.Writer) error {
	return a.archive.Stream(ctx, w)
}

// Progress returns the current progress of the archive.
func (a *Archive) Progress() *progress.Progress {
	return a.archive.Progress
}
