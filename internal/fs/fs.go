package fs

import (
	"context"
	"sync"
	"time"

	"github.com/axiomhq/axiom-go/axiom"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/query"
	"github.com/axiomhq/axiom-fs/internal/store"
)

type FS struct {
	Config   config.Config
	Client   axiomclient.API
	Executor query.Runner
	Store    *store.QueryStore

	datasets datasetCache
	fields   fieldCache
}

func New(cfg config.Config, client axiomclient.API, executor query.Runner) *FS {
	return &FS{
		Config:   cfg,
		Client:   client,
		Executor: executor,
		Store:    store.NewQueryStore(cfg.QueryDir),
		datasets: datasetCache{ttl: 30 * time.Second},
		fields:   fieldCache{ttl: 30 * time.Second},
	}
}

func (f *FS) Root() *Root {
	return &Root{fsys: f}
}

type datasetCache struct {
	mu       sync.Mutex
	fetched  time.Time
	datasets []*axiom.Dataset
	ttl      time.Duration
}

type fieldCache struct {
	mu      sync.Mutex
	fetched map[string]time.Time
	fields  map[string][]string
	ttl     time.Duration
}

func (c *fieldCache) List(ctx context.Context, fsys *FS, dataset string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fields == nil {
		c.fields = make(map[string][]string)
		c.fetched = make(map[string]time.Time)
	}
	if ts, ok := c.fetched[dataset]; ok && time.Since(ts) < c.ttl {
		return c.fields[dataset], nil
	}

	fields, err := fetchFields(ctx, fsys, dataset)
	if err != nil {
		return nil, err
	}
	c.fields[dataset] = fields
	c.fetched[dataset] = time.Now()
	return fields, nil
}

func (c *datasetCache) List(ctx context.Context, client axiomclient.API) ([]*axiom.Dataset, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Since(c.fetched) < c.ttl && len(c.datasets) > 0 {
		return c.datasets, nil
	}
	datasets, err := client.ListDatasets(ctx)
	if err != nil {
		return nil, err
	}
	c.datasets = datasets
	c.fetched = time.Now()
	return datasets, nil
}
