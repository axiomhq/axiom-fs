package vfs

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

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
	cacheDir := cfg.CacheDir
	if cacheDir != "" {
		_ = os.MkdirAll(filepath.Join(cacheDir, "fields"), 0o755)
	}
	fsys := &FS{
		Config:   cfg,
		Client:   client,
		Executor: executor,
		Store:    store.NewQueryStore(cfg.QueryDir),
		datasets: datasetCache{ttl: cfg.MetadataTTL, dir: cacheDir},
		fields:   fieldCache{ttl: cfg.MetadataTTL, dir: cacheDir},
	}
	return &Root{fsys: fsys}
}

type datasetCache struct {
	mu       sync.RWMutex
	fetched  time.Time
	datasets []axiomclient.Dataset
	ttl      time.Duration
	dir      string
	sf       singleflight.Group
}

type fieldCache struct {
	mu      sync.RWMutex
	fetched map[string]time.Time
	fields  map[string][]axiomclient.Field
	ttl     time.Duration
	dir     string
	sf      singleflight.Group
}

func (c *datasetCache) List(ctx context.Context, client axiomclient.API) ([]axiomclient.Dataset, error) {
	c.mu.RLock()
	if time.Since(c.fetched) < c.ttl && len(c.datasets) > 0 {
		datasets := c.datasets
		c.mu.RUnlock()
		return datasets, nil
	}
	c.mu.RUnlock()

	// Try loading from disk if memory cache is empty
	if datasets, ok := c.loadDisk(); ok {
		c.mu.Lock()
		c.datasets = datasets
		c.fetched = time.Now()
		c.mu.Unlock()
		return datasets, nil
	}

	result, err, _ := c.sf.Do("datasets", func() (any, error) {
		datasets, err := client.ListDatasets(ctx)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.datasets = datasets
		c.fetched = time.Now()
		c.mu.Unlock()
		if err := c.saveDisk(datasets); err != nil {
			slog.Warn("failed to cache datasets", "error", err)
		}
		return datasets, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]axiomclient.Dataset), nil
}

func (c *datasetCache) diskPath() string {
	if c.dir == "" {
		return ""
	}
	return filepath.Join(c.dir, "datasets.json")
}

func (c *datasetCache) loadDisk() ([]axiomclient.Dataset, bool) {
	path := c.diskPath()
	if path == "" {
		return nil, false
	}
	info, err := os.Stat(path)
	if err != nil || time.Since(info.ModTime()) > c.ttl {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var datasets []axiomclient.Dataset
	if json.Unmarshal(data, &datasets) != nil {
		return nil, false
	}
	return datasets, true
}

func (c *datasetCache) saveDisk(datasets []axiomclient.Dataset) error {
	path := c.diskPath()
	if path == "" {
		return nil
	}
	data, err := json.Marshal(datasets)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (c *fieldCache) List(ctx context.Context, client axiomclient.API, dataset string) ([]axiomclient.Field, error) {
	c.mu.RLock()
	if c.fields != nil {
		if ts, ok := c.fetched[dataset]; ok && time.Since(ts) < c.ttl {
			fields := c.fields[dataset]
			c.mu.RUnlock()
			return fields, nil
		}
	}
	c.mu.RUnlock()

	// Try loading from disk
	if fields, ok := c.loadDisk(dataset); ok {
		c.mu.Lock()
		if c.fields == nil {
			c.fields = make(map[string][]axiomclient.Field)
			c.fetched = make(map[string]time.Time)
		}
		c.fields[dataset] = fields
		c.fetched[dataset] = time.Now()
		c.mu.Unlock()
		return fields, nil
	}

	result, err, _ := c.sf.Do("fields:"+dataset, func() (any, error) {
		fields, err := client.ListFields(ctx, dataset)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.fields == nil {
			c.fields = make(map[string][]axiomclient.Field)
			c.fetched = make(map[string]time.Time)
		}
		c.fields[dataset] = fields
		c.fetched[dataset] = time.Now()
		c.mu.Unlock()
		if err := c.saveDisk(dataset, fields); err != nil {
			slog.Warn("failed to cache fields", "dataset", dataset, "error", err)
		}
		return fields, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]axiomclient.Field), nil
}

func (c *fieldCache) diskPath(dataset string) string {
	if c.dir == "" {
		return ""
	}
	return filepath.Join(c.dir, "fields", dataset+".json")
}

func (c *fieldCache) loadDisk(dataset string) ([]axiomclient.Field, bool) {
	path := c.diskPath(dataset)
	if path == "" {
		return nil, false
	}
	info, err := os.Stat(path)
	if err != nil || time.Since(info.ModTime()) > c.ttl {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var fields []axiomclient.Field
	if json.Unmarshal(data, &fields) != nil {
		return nil, false
	}
	return fields, true
}

func (c *fieldCache) saveDisk(dataset string, fields []axiomclient.Field) error {
	path := c.diskPath(dataset)
	if path == "" {
		return nil
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (c *fieldCache) Lookup(ctx context.Context, client axiomclient.API, dataset, fieldName string) (axiomclient.Field, bool, error) {
	fields, err := c.List(ctx, client, dataset)
	if err != nil {
		return axiomclient.Field{}, false, err
	}
	for _, f := range fields {
		if f.Name == fieldName {
			return f, true, nil
		}
	}
	return axiomclient.Field{}, false, nil
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
		if dataset.Name == "" {
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

func (r *Root) lookupDataset(ctx context.Context, name string) (*axiomclient.Dataset, error) {
	datasets, err := r.fsys.datasets.List(ctx, r.fsys.Client)
	if err != nil {
		return nil, err
	}
	for i := range datasets {
		if datasets[i].Name == name {
			return &datasets[i], nil
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
