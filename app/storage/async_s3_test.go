package storage

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestAsyncUploaderStopPreventsRetryAfterShutdown(t *testing.T) {
	t.Parallel()

	putStarted := make(chan struct{}, 1)
	uploader := NewAsyncUploader(
		&mockBackend{
			putFunc: func(ctx context.Context, key string, r io.Reader, size int64) error {
				select {
				case putStarted <- struct{}{}:
				default:
				}
				return errors.New("upload failed")
			},
		},
		&mockBackend{
			getFunc: func(ctx context.Context, key string) (io.ReadCloser, int64, time.Time, bool, error) {
				return stringReadCloser("data"), 4, time.Time{}, true, nil
			},
		},
		8,
		3,
	)
	uploader.retryDelay = func(int) time.Duration { return 10 * time.Millisecond }
	uploader.Start(1)

	if !uploader.Enqueue("k", 4) {
		t.Fatal("expected enqueue to succeed")
	}

	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial upload attempt")
	}

	uploader.Stop()
	time.Sleep(30 * time.Millisecond)

	if uploader.Enqueue("k", 4) {
		t.Fatal("expected enqueue to fail after shutdown")
	}
}
