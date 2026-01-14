package vfs

import (
	"context"
	"os"
	"sort"
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

func NewRoot(cfg config.Config, client axiomclient.API, executor query.Runner) *Root {
	fsys := &FS{
		Config:   cfg,
		Client:   client,
		Executor: executor,
		Store:    store.NewQueryStore(cfg.QueryDir),
		datasets: datasetCache{ttl: 30 * time.Second},
		fields:   fieldCache{ttl: 30 * time.Second},
	}
	return &Root{fsys: fsys}
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

func (c *fieldCache) List(ctx context.Context, root *Root, dataset string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fields == nil {
		c.fields = make(map[string][]string)
		c.fetched = make(map[string]time.Time)
	}
	if ts, ok := c.fetched[dataset]; ok && time.Since(ts) < c.ttl {
		return c.fields[dataset], nil
	}

	fields, err := fetchFields(ctx, root, dataset)
	if err != nil {
		return nil, err
	}
	c.fields[dataset] = fields
	c.fetched[dataset] = time.Now()
	return fields, nil
}

type Root struct {
	fsys *FS
}

func (r *Root) Config() config.Config    { return r.fsys.Config }
func (r *Root) Client() axiomclient.API  { return r.fsys.Client }
func (r *Root) Executor() query.Runner   { return r.fsys.Executor }
func (r *Root) Store() *store.QueryStore { return r.fsys.Store }

func (r *Root) datasets() *datasetCache { return &r.fsys.datasets }
func (r *Root) fields() *fieldCache     { return &r.fsys.fields }

func (r *Root) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo(""), nil
}

func (r *Root) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	entries := []os.FileInfo{
		DirInfo("datasets"),
		FileInfo("README.txt", 0),
		DirInfo("examples"),
		DirInfo("_presets"),
		DirInfo("_queries"),
	}

	datasets, err := r.fsys.datasets.List(ctx, r.fsys.Client)
	if err != nil {
		return nil, err
	}
	for _, dataset := range datasets {
		if dataset == nil || dataset.Name == "" {
			continue
		}
		if isReservedRoot(dataset.Name) {
			continue
		}
		entries = append(entries, DirInfo(dataset.Name))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (r *Root) Lookup(ctx context.Context, name string) (Node, error) {
	switch name {
	case "README.txt":
		return &StaticFile{name: name, data: readmeText}, nil
	case "examples":
		return &ExamplesDir{}, nil
	case "datasets":
		return &DatasetsDir{root: r}, nil
	case "_presets":
		return &PresetsDir{}, nil
	case "_queries":
		return &QueriesDir{root: r}, nil
	}

	dataset, err := r.lookupDataset(ctx, name)
	if err != nil {
		return nil, err
	}
	if dataset == nil {
		return nil, os.ErrNotExist
	}
	return &DatasetDir{root: r, dataset: dataset}, nil
}

func (r *Root) lookupDataset(ctx context.Context, name string) (*axiom.Dataset, error) {
	datasets, err := r.fsys.datasets.List(ctx, r.fsys.Client)
	if err != nil {
		return nil, err
	}
	for _, dataset := range datasets {
		if dataset != nil && dataset.Name == name {
			return dataset, nil
		}
	}
	return nil, nil
}

func isReservedRoot(name string) bool {
	switch name {
	case "datasets", "README.txt", "examples", "_presets", "_queries":
		return true
	default:
		return false
	}
}
