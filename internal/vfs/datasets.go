package vfs

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/axiomhq/axiom-go/axiom"
	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/query"
)

type DatasetsDir struct {
	root *Root
}

func (d *DatasetsDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("datasets"), nil
}

func (d *DatasetsDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	datasets, err := d.root.datasets().List(ctx, d.root.Client())
	if err != nil {
		return nil, err
	}
	entries := make([]os.FileInfo, 0, len(datasets))
	for _, dataset := range datasets {
		if dataset == nil || dataset.Name == "" {
			continue
		}
		entries = append(entries, DirInfo(dataset.Name))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (d *DatasetsDir) Lookup(ctx context.Context, name string) (Node, error) {
	datasets, err := d.root.datasets().List(ctx, d.root.Client())
	if err != nil {
		return nil, err
	}
	for _, dataset := range datasets {
		if dataset != nil && dataset.Name == name {
			return &DatasetDir{root: d.root, dataset: dataset}, nil
		}
	}
	return nil, os.ErrNotExist
}

type DatasetDir struct {
	root    *Root
	dataset *axiom.Dataset
}

func (d *DatasetDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo(d.dataset.Name), nil
}

func (d *DatasetDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	return []os.FileInfo{
		FileInfo("schema.json", 0),
		FileInfo("schema.csv", 0),
		FileInfo("sample.ndjson", 0),
		DirInfo("fields"),
		DirInfo("presets"),
		DirInfo("q"),
	}, nil
}

func (d *DatasetDir) Lookup(ctx context.Context, name string) (Node, error) {
	switch name {
	case "schema.json":
		return &DatasetSchemaFile{root: d.root, dataset: d.dataset, format: "json"}, nil
	case "schema.csv":
		return &DatasetSchemaFile{root: d.root, dataset: d.dataset, format: "csv"}, nil
	case "sample.ndjson":
		return &DatasetSampleFile{root: d.root, dataset: d.dataset}, nil
	case "fields":
		return &FieldsDir{root: d.root, dataset: d.dataset}, nil
	case "presets":
		return &DatasetPresetsDir{root: d.root, dataset: d.dataset}, nil
	case "q":
		return &QueryPathDir{root: d.root, dataset: d.dataset.Name, segments: nil}, nil
	default:
		return nil, os.ErrNotExist
	}
}

type FieldsDir struct {
	root    *Root
	dataset *axiom.Dataset
}

func (f *FieldsDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("fields"), nil
}

func (f *FieldsDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	fields, err := f.root.fields().List(ctx, f.root, f.dataset.Name)
	if err != nil {
		return nil, err
	}
	entries := make([]os.FileInfo, 0, len(fields))
	for _, field := range fields {
		entries = append(entries, DirInfo(field))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (f *FieldsDir) Lookup(ctx context.Context, name string) (Node, error) {
	return &FieldDir{root: f.root, dataset: f.dataset, field: name}, nil
}

type FieldDir struct {
	root    *Root
	dataset *axiom.Dataset
	field   string
}

func (f *FieldDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo(f.field), nil
}

func (f *FieldDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	return []os.FileInfo{
		FileInfo("top.csv", 0),
		FileInfo("histogram.csv", 0),
	}, nil
}

func (f *FieldDir) Lookup(ctx context.Context, name string) (Node, error) {
	switch name {
	case "top.csv":
		return &FieldQueryFile{root: f.root, dataset: f.dataset, field: f.field, kind: "top"}, nil
	case "histogram.csv":
		return &FieldQueryFile{root: f.root, dataset: f.dataset, field: f.field, kind: "histogram"}, nil
	default:
		return nil, os.ErrNotExist
	}
}

type DatasetSchemaFile struct {
	root    *Root
	dataset *axiom.Dataset
	format  string
}

func (d *DatasetSchemaFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("schema."+d.format, 0), nil
}

func (d *DatasetSchemaFile) Open(ctx context.Context, flags int) (billy.File, error) {
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| getschema",
		d.dataset.Name,
		d.root.Config().DefaultRange,
	)
	data, err := d.root.Executor().ExecuteAPL(ctx, apl, d.format, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}

type DatasetSampleFile struct {
	root    *Root
	dataset *axiom.Dataset
}

func (d *DatasetSampleFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("sample.ndjson", 0), nil
}

func (d *DatasetSampleFile) Open(ctx context.Context, flags int) (billy.File, error) {
	cfg := d.root.Config()
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| take %d",
		d.dataset.Name,
		cfg.DefaultRange,
		cfg.SampleLimit,
	)
	data, err := d.root.Executor().ExecuteAPL(ctx, apl, "ndjson", query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}

type FieldQueryFile struct {
	root    *Root
	dataset *axiom.Dataset
	field   string
	kind    string
}

func (f *FieldQueryFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo(f.kind+".csv", 0), nil
}

func (f *FieldQueryFile) Open(ctx context.Context, flags int) (billy.File, error) {
	var expr string
	switch f.kind {
	case "top":
		expr = fmt.Sprintf("summarize topk(%s, 10)", f.field)
	case "histogram":
		expr = fmt.Sprintf("summarize histogram(%s, 100)", f.field)
	default:
		return nil, os.ErrInvalid
	}
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| %s",
		f.dataset.Name,
		f.root.Config().DefaultRange,
		expr,
	)
	data, err := f.root.Executor().ExecuteAPL(ctx, apl, "csv", query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}
