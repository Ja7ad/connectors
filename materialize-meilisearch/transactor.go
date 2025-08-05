package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/meilisearch/meilisearch-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"sync"
)

var _ boilerplate.MaterializerTransactor = (*transactor)(nil)

const (
	_batchSize      = 100
	_loadWorkers    = 5
	_storeWorkers   = 5
	_storeBatchSize = 5 * 1024 * 1024
)

type transactor struct {
	cfg      config
	client   meilisearch.ServiceManager
	bindings []*binding
}

type binding struct {
	uid          string
	index        meilisearch.IndexManager
	deltaUpdates bool
}

type loadBatch struct {
	id       int
	indexUID string
	doc      meilisearch.DocumentReader
	ids      []string
}

type storeBatch struct {
	index string
	buf   []byte
}

func (t *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, spec2 pf.RangeSpec) (boilerplate.RuntimeCheckpoint, error) {
	return nil, nil // No persisted state — Flow handles recovery via its logs.
}

func (t *transactor) UnmarshalState(_ json.RawMessage) error { return nil }

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(binding int, doc json.RawMessage) error) error {
	ctx := it.Context()
	it.WaitForAcknowledged()

	var mu sync.Mutex
	loadFn := func(b int, d json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()
		return loaded(b, d)
	}

	gp, gpCtx := errgroup.WithContext(ctx)
	reqCh := make(chan loadBatch)

	for i := 0; i < _loadWorkers; i++ {
		gp.Go(t.loadWorker(gpCtx, loadFn, reqCh))
	}

	sendBatch := func(bind int, indexUID string, doc meilisearch.DocumentReader, ids []string) error {
		select {
		case <-gpCtx.Done():
			return gp.Wait()
		case reqCh <- loadBatch{id: bind, indexUID: indexUID, doc: doc, ids: ids}:
			return nil
		}
	}

	batches := make([][]string, len(t.bindings))

	for it.Next() {
		key := base64.RawStdEncoding.EncodeToString(it.PackedKey)
		bind := it.Binding
		batches[bind] = append(batches[bind], key)

		if len(batches[bind]) >= _batchSize {
			ids := batches[bind]
			if err := sendBatch(bind, t.bindings[bind].uid, t.bindings[bind].index, ids); err != nil {
				return err
			}
			batches[bind] = nil
		}
	}

	// Drain remaining batches
	for bind, ids := range batches {
		if len(ids) > 0 {
			if err := sendBatch(bind, t.bindings[bind].uid, t.bindings[bind].index, ids); err != nil {
				return err
			}
		}
	}

	close(reqCh)
	return gp.Wait()
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	batchCh := make(chan storeBatch)
	gp, gpCtx := errgroup.WithContext(ctx)
	for range _storeWorkers {
		gp.Go(func() error {
			for {
				select {
				case <-gpCtx.Done():
					return gpCtx.Err()
				case batch, ok := <-batchCh:
					if !ok {
						return nil
					}

					if err := t.storeWorker(gpCtx, batch.index, batch.buf); err != nil {
						return err
					}
				}
			}
		})
	}

	sendBatchFunc := func(index string, buf []byte) error {
		select {
		case <-gpCtx.Done():
			return gpCtx.Err()
		case batchCh <- storeBatch{index: index, buf: buf}:
			return nil
		}
	}

	buf := make([]byte, 0)
	lastIndex := ""

	for it.Next() {
		b := t.bindings[it.Binding]

		if len(buf) > _storeBatchSize || (lastIndex != b.uid && lastIndex != "") {
			if err := sendBatchFunc(lastIndex, buf); err != nil {
				return nil, err
			}
		}
		lastIndex = b.uid

		primaryKey := base64.RawStdEncoding.EncodeToString(it.PackedKey)

		if it.Delete && t.cfg.HardDelete {

		}
	}
}

func (t *transactor) Destroy() {
	//TODO implement me
	panic("implement me")
}

func (t *transactor) loadWorker(
	ctx context.Context,
	load func(i int, doc json.RawMessage) error,
	reqCh <-chan loadBatch,
) func() error {
	return func() error {
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("operation cancelled: %w", ctx.Err())
			case batch, ok := <-reqCh:
				if !ok {
					return nil
				}

				for attempt := 1; ; attempt++ {
					var resp meilisearch.DocumentsResult
					err := batch.doc.GetDocumentsWithContext(ctx, &meilisearch.DocumentsQuery{
						Ids: batch.ids,
					}, &resp)
					if err == nil {
						for _, res := range resp.Results {
							b, err := json.Marshal(res)
							if err != nil {
								return fmt.Errorf("failed encoding document in index %s: %w", batch.indexUID, err)
							}
							if err := load(batch.id, b); err != nil {
								return fmt.Errorf("failed load document: %w", err)
							}
						}
						break // Success, exit retry loop
					}

					log.Debugf("failed to get batch document from meiliesarch: %v", err)

					if err := delay(ctx, attempt, "load", err); err != nil {
						return err
					}
				}
			}
		}
	}
}

func (t *transactor) storeWorker(
	ctx context.Context,
	index string,
	body []byte,
) error {
	for attempt := 1; ; attempt++ {
		idx := t.client.Index(index)

		taskInfo, err := idx.AddDocumentsWithContext(ctx, body, nil)
		if err != nil {
			log.Debugf("failed to add documents to index %s: %v", index, err)
		} else {
			task, err := idx.WaitForTaskWithContext(ctx, taskInfo.TaskUID, 0)
			if err != nil {
				log.Debugf("failed to wait for task in index %s: %v", index, err)
			} else if task.Status == meilisearch.TaskStatusSucceeded {
				return nil // Success
			} else {
				err = fmt.Errorf("task did not succeed: status=%s", task.Status)
				log.Debugf("task in index %s did not succeed: %v", index, err)
			}
		}

		if err := delay(ctx, attempt, "store", err); err != nil {
			return err
		}
	}
}
