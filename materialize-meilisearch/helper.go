package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

const (
	// Initial backoff to use if a batch request returns successfully with unprocessed items/keys.
	// Will increase exponentially as additional requests continue to return unprocessed items/keys.
	initialBackoff = 10 * time.Millisecond

	// Maximum amount of time to wait between retry attempts of unprocessed items/keys.
	maxBackoff = 1 * time.Second

	// Maximum number of retry attempts before failing with an error.
	maxAttempts = 10
)

func delay(ctx context.Context, attempt int, key string, err error) error {
	if attempt > maxAttempts {
		return fmt.Errorf("%s worker failed after %d retry attempts, got err: %w", key, maxAttempts, err)
	}

	d := time.Duration(1<<attempt) * initialBackoff
	if d > maxBackoff {
		d = maxBackoff
	}

	entry := log.WithFields(log.Fields{
		"attempt":  attempt,
		"waitTime": d.String(),
	})
	msg := fmt.Sprintf("%s worker waiting to retry unprocessed items", key)

	if attempt < 15 {
		entry.Debug(msg)
	} else {
		entry.Info(msg)
	}

	// Retry with exponential backoff if there were unprocessed items in the batch.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
