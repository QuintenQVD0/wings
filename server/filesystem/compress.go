package filesystem

import (
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/klauspost/compress/zip"
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

func (fs *Filesystem) archiverFileSystem(ctx context.Context, p string) (iofs.FS, io.Closer, error) {
	f, err := fs.unixFS.Open(p)
	if err != nil {
		return nil, nil, err
	}
	// Do not use defer to close `f`, it will likely be used later.

	format, _, err := archives.Identify(ctx, filepath.Base(p), f)
	if err != nil && !errors.Is(err, archives.NoMatch) {
		_ = f.Close()
		return nil, nil, err
	}

	// Reset the file reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	if format != nil {
		switch ff := format.(type) {
		case archives.Zip:
			// zip.Reader is more performant than ArchiveFS, because zip.Reader caches content information
			// and zip.Reader can open several content files concurrently because of io.ReaderAt requirement
			// while ArchiveFS can't.
			// zip.Reader doesn't suffer from issue #330 and #310 according to local test (but they should be fixed anyway)
			reader, err := zip.NewReader(f, info.Size())
			if err != nil {
				_ = f.Close()
				return nil, nil, err
			}
			return reader, f, nil
		case archives.Extraction:
			return &archives.ArchiveFS{Stream: io.NewSectionReader(f, 0, info.Size()), Format: ff, Context: ctx}, f, nil
		case archives.Compression:
			return archiverext.FileFS{File: f, Compression: ff}, f, nil
		}
	}
	_ = f.Close()
	return nil, nil, archives.NoMatch
}

// SpaceAvailableForDecompression looks through a given archive and determines
// if decompressing it would put the server over its allocated disk space limit.
func (fs *Filesystem) SpaceAvailableForDecompression(ctx context.Context, dir string, file string) error {
	// Don't waste time trying to determine this if we know the server will have the space for
	// it since there is no limit.
	if fs.MaxDisk() <= 0 {
		return nil
	}

	fsys, archive, err := fs.archiverFileSystem(ctx, filepath.Join(dir, file))
	if err != nil {
		if errors.Is(err, archives.NoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}
	defer archive.Close()

	var size atomic.Int64
	return iofs.WalkDir(fsys, ".", func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			// Stop walking if the context is canceled.
			return ctx.Err()
		default:
			info, err := d.Info()
			if err != nil {
				return err
			}
			if !fs.unixFS.CanFit(size.Add(info.Size())) {
				return newFilesystemError(ErrCodeDiskSpace, nil)
			}
			return nil
		}
	})
}

// DecompressFile will decompress a file in a given directory by using the
// archiver tool to infer the file type and go from there. This will walk over
// all the files within the given archive and ensure that there is not a
// zip-slip attack being attempted by validating that the final path is within
// the server data directory.
func (fs *Filesystem) DecompressFile(ctx context.Context, dir string, file string) error {
	f, err := fs.unixFS.Open(filepath.Join(dir, file))
	if err != nil {
		return err
	}
	defer f.Close()

	// Identify the type of archive we are dealing with.
	format, input, err := archives.Identify(ctx, filepath.Base(file), f)
	if err != nil {
		if errors.Is(err, archives.NoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}

	return fs.extractStream(ctx, extractStreamOptions{
		FileName:  file,
		Directory: dir,
		Format:    format,
		Reader:    input,
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
	return fs.extractStream(ctx, extractStreamOptions{
		Directory: dir,
		Format:    format,
		Reader:    input,
	})
}

type extractStreamOptions struct {
	// The directory to extract the archive to.
	Directory string
	// File name of the archive.
	FileName string
	// Format of the archive.
	Format archives.Format
	// Reader for the archive.
	Reader io.Reader
}

func (fs *Filesystem) extractStream(ctx context.Context, opts extractStreamOptions) error {
	// See if it's a compressed archive, such as TAR or a ZIP
	ex, ok := opts.Format.(archives.Extractor)
	if !ok {
		// If not, check if it's a single-file compression, such as
		// .log.gz, .sql.gz, and so on
		de, ok := opts.Format.(archives.Decompressor)
		if !ok {
			return nil
		}

		// Strip the compression suffix
		p := filepath.Join(opts.Directory, strings.TrimSuffix(opts.FileName, opts.Format.Extension()))

		// Make sure it's not ignored
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}

		reader, err := de.OpenReader(opts.Reader)
		if err != nil {
			return err
		}
		defer reader.Close()

		// Open the file for creation/writing
		f, err := fs.unixFS.OpenFile(p, ufs.O_WRONLY|ufs.O_CREATE, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()

		// Read in 4 KB chunks
		buf := make([]byte, 4096)
		for {
			n, err := reader.Read(buf)
			if n > 0 {

				// Check quota before writing the chunk
				if quotaErr := fs.HasSpaceFor(int64(n)); quotaErr != nil {
					return quotaErr
				}

				// Write the chunk
				if _, writeErr := f.Write(buf[:n]); writeErr != nil {
					return writeErr
				}

				// Add to quota
				fs.addDisk(int64(n))
			}

			if err != nil {
				// EOF are expected
				if err == io.EOF {
					break
				}

				// Return any other
				return err
			}
		}

		return nil
	}

	// Decompress and extract archive
	return ex.Extract(ctx, opts.Reader, func(ctx context.Context, f archives.FileInfo) error {
		p := filepath.Join(opts.Directory, f.NameInArchive)
		// If it is ignored, just don't do anything with the entry and skip over it.
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}
		// Create directories explicitly; an empty one has no file to create it
		// implicitly and would otherwise be dropped during extraction.
		if f.IsDir() {
			if err := fs.mkdirAll(p, 0o755); err != nil {
				return wrapError(err, opts.FileName)
			}
			return nil
		}
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()
		if err := fs.Write(p, r, f.Size(), f.Mode()); err != nil {
			return wrapError(err, opts.FileName)
		}
		// Update the file modification time to the one set in the archive.
		if err := fs.Chtimes(p, f.ModTime(), f.ModTime()); err != nil {
			return wrapError(err, opts.FileName)
		}
		return nil
	})
}
