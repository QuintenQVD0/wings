package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"
)

// PushArchiveToTarget POSTs the archive to the target node and returns the
// response body.
func (t *Transfer) PushArchiveToTarget(url, token string, backups []string) ([]byte, error) {
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()

	t.SendMessage("Preparing to stream server data to destination...")
	t.SetStatus(StatusProcessing)

	a, err := t.Archive()
	if err != nil {
		t.Error(err, "Failed to get archive for transfer.")
		return nil, errors.New("failed to get archive for transfer")
	}

	t.SendMessage("Streaming archive to destination...")

	// Send the upload progress to the websocket every 5 seconds.
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	go func(ctx context.Context, a *Archive, tc *time.Ticker) {
		defer tc.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tc.C:
				progress := a.Progress()
				if progress != nil {
					message := "Uploading " + progress.Progress(25)
					// We can't easily show backup count here without tracking totalBackups
					// But we're already showing individual backup progress in StreamBackups
					t.SendMessage(message)
					t.Log().Info(message)
				}
			}
		}
	}(ctx2, a, time.NewTicker(5*time.Second))

	// Create a new request using the pipe as the body.
	body, writer := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		body.Close()
		writer.Close()
		return nil, err
	}
	req.Header.Set("Authorization", token)

	// Create a new multipart writer that writes the archive to the pipe.
	// NOTE: Do NOT defer mp.Close() here. The goroutine below owns the lifecycle
	// of mp and writer. Closing them from outside would corrupt the stream.
	mp := multipart.NewWriter(writer)
	req.Header.Set("Content-Type", mp.FormDataContentType())

	// errChan receives the result of the streaming goroutine.
	errChan := make(chan error, 1)
	go func() {
		// This goroutine exclusively owns mp and writer.
		// It must close both exactly once, in the right order:
		//   1. mp.Close() — writes the multipart terminating boundary into writer
		//   2. writer.Close() — signals EOF to the HTTP request body reader
		var streamErr error
		defer func() {
			// Step 1: finalize the multipart stream
			if err := mp.Close(); err != nil && streamErr == nil {
				streamErr = fmt.Errorf("failed to close multipart writer: %w", err)
			}
			t.Log().Debug("closed multipart writer")

			// Step 2: signal EOF to the HTTP body — must happen after mp.Close()
			writer.Close()

			// Step 3: report any error (or nil for success)
			errChan <- streamErr
			close(errChan)
		}()

		// --- Stream the main archive ---

		src, pw := io.Pipe()

		mainHasher := sha256.New()
		mainTee := io.TeeReader(src, mainHasher)

		dest, err := mp.CreateFormFile("archive", "archive.tar.gz")
		if err != nil {
			src.Close()
			pw.Close()
			streamErr = errors.New("failed to create archive form file")
			return
		}

		// Goroutine to copy from the tee (hasher side) into the multipart field.
		ch := make(chan error, 1)
		go func() {
			_, copyErr := io.Copy(dest, mainTee)
			if copyErr != nil {
				ch <- fmt.Errorf("failed to copy archive to destination: %w", copyErr)
			} else {
				t.Log().Debug("finished copying main archive to destination")
				ch <- nil
			}
			close(ch)
		}()

		// Stream the actual server data into pw; the tee goroutine above reads from src.
		if err := a.Stream(ctx, pw); err != nil {
			pw.Close()
			src.Close()
			// Drain ch so its goroutine doesn't leak.
			<-ch
			streamErr = fmt.Errorf("failed to stream archive to pipe: %w", err)
			return
		}
		t.Log().Debug("finished streaming archive to pipe")

		// Signal EOF to the tee reader by closing the write end.
		pw.Close()

		// Wait for the copy goroutine to finish.
		t.Log().Debug("waiting for main archive copy to finish")
		if err := <-ch; err != nil {
			src.Close()
			streamErr = err
			return
		}
		src.Close()

		// --- Write the archive checksum field ---
		if err := mp.WriteField("checksum_archive", hex.EncodeToString(mainHasher.Sum(nil))); err != nil {
			streamErr = fmt.Errorf("failed to write archive checksum field: %w", err)
			return
		}
		t.Log().Debug("wrote archive checksum field")

		// --- Stream backups ---
		t.BackupUUIDs = backups
		if len(t.BackupUUIDs) > 0 {
			t.SendMessage(fmt.Sprintf("Streaming %d backup files to destination...", len(t.BackupUUIDs)))
			if err := a.StreamBackups(ctx, mp); err != nil {
				streamErr = fmt.Errorf("failed to stream backups: %w", err)
				return
			}
		} else {
			t.Log().Debug("no backups specified for transfer")
		}

		// Stop the progress ticker — we're done with the main upload phase.
		cancel2()
		t.SendMessage("Finished streaming archive and backups to destination.")

		// --- Stream install logs (optional) ---
		if err := a.StreamInstallLogs(ctx, mp); err != nil {
			streamErr = fmt.Errorf("failed to stream install logs: %w", err)
			return
		}
		t.SendMessage("Finished streaming install logs to destination.")

		// streamErr is nil here; the deferred close will finalize and signal success.
		t.Log().Debug("all parts written, finalizing multipart stream")
	}()

	t.Log().Debug("sending archive to destination")
	client := http.Client{Timeout: 0}
	res, err := client.Do(req)

	// client.Do has returned — the HTTP transport will no longer read from body.
	// We must unblock the streaming goroutine which may be stuck writing into the
	// pipe (e.g. in a.Stream, io.Copy, or mp.WriteField):
	//   1. Cancel the context so context-aware operations (a.Stream, a.StreamBackups)
	//      return immediately.
	//   2. Close the pipe read end so any non-context-aware writes get a broken pipe
	//      error and return instead of blocking forever.
	cancel()
	body.Close()

	// Now it is safe to wait — the goroutine will unblock and exit promptly.
	t.Log().Debug("waiting for streaming goroutine to complete")
	streamErr := <-errChan
	t.Log().Debug("streaming goroutine completed")

	if err != nil {
		t.Log().WithError(err).Debug("HTTP error while sending archive to destination")
		return nil, fmt.Errorf("failed to send archive to destination: %w", err)
	}

	if streamErr != nil {
		if streamErr == context.Canceled {
			return nil, streamErr
		}
		t.Log().WithError(streamErr).Debug("streaming error while sending archive to destination")
		return nil, fmt.Errorf("streaming error: %w", streamErr)
	}

	if res.StatusCode != http.StatusOK {
		defer res.Body.Close()
		v, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("unexpected status code from destination: %d — %s", res.StatusCode, string(v))
	}

	defer res.Body.Close()
	t.Log().Debug("received successful response from destination")

	v, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return v, nil
}
