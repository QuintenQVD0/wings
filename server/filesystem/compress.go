package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/mholt/archives"
	"github.com/pelican-dev/wings/internal"
)

type extractOptions struct {
	dir    string
	file   string
	format archives.Format
	r      io.Reader
}

// CompressFiles compresses all the files matching the given paths in the
// specified directory. This function also supports passing nested paths to only
// compress certain files and folders when working in a larger directory. This
// effectively creates a local backup, but rather than ignoring specific files
// and folders, it takes an allowlist of files and folders.
//
// All paths are relative to the dir that is passed in as the first argument,
// and the compressed file will be placed at that location named
// `archive-{date}.tar.gz`.
func (fs *Filesystem) CompressFiles(ctx context.Context, dir string, name string, paths []string, extension string) (os.FileInfo, string, error) {
	// Build the archive instance purely to reuse its validation + matcher
	// construction (WithMatching handles the ignored/matching mutual-exclusion
	// check and the leading-slash allowlist semantics). 
	a, err := NewArchive(fs.root, dir, WithMatching(paths))
	if err != nil {
		return nil, "", errors.WrapIf(err, "server/filesystem: compress: failed to create archive instance")
	}

	// Normalize extension & assign MIME type
	extension = strings.ToLower(strings.TrimPrefix(extension, "."))
	var (
		ext      string
		mimetype string
	)
	switch extension {
	case "zip":
		ext = ".zip"
		mimetype = "application/zip"
	case "tar.gz", "tgz":
		ext = ".tar.gz"
		mimetype = "application/gzip"
	case "tar.bz2", "tbz2":
		ext = ".tar.bz2"
		mimetype = "application/x-bzip2"
	case "tar.xz", "txz":
		ext = ".tar.xz"
		mimetype = "application/x-xz"
	default:
		// fallback to tar.gz
		ext = ".tar.gz"
		mimetype = "application/gzip"
	}

	if name == "" {
		name = fmt.Sprintf("archive-%s%s", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", ""), ext)
	} else {
		name, err = fs.findCopySuffix(dir, name, ext)
		if err != nil {
			return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to find unique archive name")
		}
	}

	destPath := normalize(filepath.Join(dir, name))

	//   1. fs.denylist (IsIgnored) - server-level denylist, always excluded
	//   2. a.matching - the allowlist built from paths the user actually requested
	// then against the sandboxed os.Root itself, mirroring Archive.addToArchive's
	// approach (Lstat + filepath.Join(root.Name(), p)).
	filesMap := make(map[string]string)
	for _, file := range paths {
		rel := path.Join(dir, file)

		// Server-level denylist check - silently skip denylisted files
		if err := fs.IsIgnored(rel); err != nil {
			continue
		}

		matchPath := "/" + strings.TrimPrefix(file, "/")
		if a.matching != nil && !a.matching.MatchesPath(matchPath) {
			continue
		}

		normalized := normalize(filepath.Join(dir, file))
		// This check does what SafePath used to do.
		// refuse any path that resolves outside the root directory
		if _, err := fs.root.Lstat(normalized); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to stat file")
		}

		absolutePath := filepath.Join(fs.root.Name(), normalized)
		filesMap[absolutePath] = file
	}


	if len(filesMap) == 0 {
		return nil, "", fmt.Errorf("no valid files to compress")
	}

	files, err := archives.FilesFromDisk(ctx, nil, filesMap)
	if err != nil {
		return nil, "", errors.WrapIf(err, "server/filesystem: compress: failed to map files for archive")
	}

	f, err := fs.root.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to open file for writing")
	}
	defer f.Close()

	cw := internal.NewCountedWriter(f)

	switch extension {
	case "zip":
		zipper := archives.Zip{}
		if err := zipper.Archive(ctx, cw, files); err != nil {
			return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to write zip archive")
		}
	case "tar.bz2", "tbz2":
		format := archives.CompressedArchive{
			Compression: archives.Bz2{},
			Archival:    archives.Tar{},
		}
		if err := format.Archive(ctx, cw, files); err != nil {
			return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to write tar.bz2 archive")
		}
	case "tar.xz", "txz":
		format := archives.CompressedArchive{
			Compression: archives.Xz{},
			Archival:    archives.Tar{},
		}
		if err := format.Archive(ctx, cw, files); err != nil {
			return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to write tar.xz archive")
		}
	default: // tar.gz and fallback
		format := archives.CompressedArchive{
			Compression: archives.Gz{},
			Archival:    archives.Tar{},
		}
		if err := format.Archive(ctx, cw, files); err != nil {
			return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to write tar.gz archive")
		}
	}

	if err := fs.HasSpaceFor(cw.BytesWritten()); err != nil {
		_ = fs.root.Remove(destPath)
		return nil, "", newFilesystemError(ErrorCode(ErrNoSpaceAvailable), nil)
	}
	fs.addDisk(cw.BytesWritten())

	info, err := f.Stat()
	if err != nil {
		return nil, "", errors.Wrap(err, "server/filesystem: compress: failed to stat archive")
	}
	return info, mimetype, nil
}

// DecompressFile will decompress a file in a given directory by using the
// archiver tool to infer the file type and go from there. This will walk over
// all the files within the given archive and ensure that there is not a
// zip-slip attack being attempted by validating that the final path is within
// the server data directory.
func (fs *Filesystem) DecompressFile(ctx context.Context, dir string, file string) error {
	f, err := fs.root.Open(normalize(filepath.Join(dir, file)))
	if err != nil {
		return errors.Wrap(err, "server/filesystem: decompress: failed to open file")
	}
	defer f.Close()

	format, input, err := archives.Identify(ctx, filepath.Base(file), f)
	if err != nil {
		if errors.Is(err, archives.NoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return errors.Wrap(err, "server/filesystem: decompress: failed to identify archive format")
	}

	return fs.extractStream(ctx, extractOptions{dir: dir, file: file, format: format, r: input})
}

func (fs *Filesystem) extractStream(ctx context.Context, opts extractOptions) error {
	// See if it's a compressed archive, such as TAR or a ZIP
	ex, ok := opts.format.(archives.Extractor)
	if !ok {
		// If not, check if it's a single-file compression, such as
		// .log.gz, .sql.gz, and so on
		de, ok := opts.format.(archives.Decompressor)
		if !ok {
			return nil
		}

		p := filepath.Join(opts.dir, strings.TrimSuffix(opts.file, opts.format.Extension()))
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}

		reader, err := de.OpenReader(opts.r)
		if err != nil {
			return errors.Wrap(err, "server/filesystem: decompress: failed to open reader")
		}
		defer reader.Close()

		// Open the file for creation/writing
		f, err := fs.root.OpenFile(normalize(p), os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			return errors.Wrap(err, "server/filesystem: decompress: failed to open file")
		}
		defer f.Close()

		// Read in 4 KB chunks
		buf := make([]byte, 4096)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				if err := fs.HasSpaceFor(int64(n)); err != nil {
					return err
				}
				if _, err := f.Write(buf[:n]); err != nil {
					return errors.Wrap(err, "server/filesystem: decompress: failed to write")
				}
				fs.addDisk(int64(n))
			}

			if err != nil {
				if err == io.EOF {
					break
				}
				return errors.Wrap(err, "server/filesystem: decompress: failed to read")
			}
		}

		return nil
	}

	// Decompress and extract archive
	return ex.Extract(ctx, opts.r, func(ctx context.Context, f archives.FileInfo) error {
		if f.IsDir() {
			return nil
		}
		p := filepath.Join(opts.dir, f.NameInArchive)
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()
		if f.Mode()&os.ModeSymlink != 0 {
			// Try to create the symlink if it is in the archive, but don't hold up the process
			// if the file cannot be created. In that case just skip over it entirely.
			if f.LinkTarget != "" {
				p2 := strings.TrimLeft(filepath.Clean(p), string(filepath.Separator))
				if p2 == "" {
					p2 = "."
				}
				// We don't use [fs.Symlink] here because that normalizes the source directory for
				// consistency with the codebase. In this case when decompressing we want to just
				// accept the source without any normalization.
				if err := fs.root.Symlink(f.LinkTarget, p2); err != nil {
					if errors.Is(err, os.ErrNotExist) || IsPathError(err) || IsLinkError(err) {
						return nil
					}
					return errors.Wrap(err, "server/filesystem: decompress: failed to create symlink")
				}
			}
			return nil
		}

		if err := fs.Write(p, r, f.Size(), f.Mode().Perm()); err != nil {
			return errors.Wrap(err, "server/filesystem: decompress: failed to write file")
		}

		// Update the file modification time to the one set in the archive.
		if err := fs.Chtimes(p, f.ModTime(), f.ModTime()); err != nil {
			return errors.Wrap(err, "server/filesystem: decompress: failed to update file modification time")
		}

		return nil
	})
}

// ExtractStreamUnsafe .
func (fs *Filesystem) ExtractStreamUnsafe(ctx context.Context, dir string, r io.Reader) error {
	format, input, err := archives.Identify(ctx, "archive.tar.gz", r)
	if err != nil {
		if errors.Is(err, archives.NoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}
	return fs.extractStream(ctx, extractOptions{
		dir:    dir,
		format: format,
		r:      input,
	})
}