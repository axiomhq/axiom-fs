package vfs

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
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
		if dataset.Name == "" {
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
	for i := range datasets {
		if datasets[i].Name == name {
			return &DatasetDir{root: d.root, dataset: &datasets[i]}, nil
		}
	}
	return nil, os.ErrNotExist
}

type DatasetDir struct {
	root    *Root
	dataset *axiomclient.Dataset
}

func (d *DatasetDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo(d.dataset.Name), nil
}

func (d *DatasetDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	// Prefetch fields in background so opening fields/ is fast
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := d.root.fields().List(ctx, d.root.Client(), d.dataset.Name); err != nil {
			slog.Warn("failed to prefetch fields", "dataset", d.dataset.Name, "error", err)
		}
	}()
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
	dataset *axiomclient.Dataset
}

func (f *FieldsDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("fields"), nil
}

func (f *FieldsDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	fields, err := f.root.fields().List(ctx, f.root.Client(), f.dataset.Name)
	if err != nil {
		return nil, err
	}
	entries := make([]os.FileInfo, 0, len(fields))
	for _, field := range fields {
		if field.Hidden {
			continue
		}
		entries = append(entries, DirInfo(field.Name))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (f *FieldsDir) Lookup(ctx context.Context, name string) (Node, error) {
	field, found, err := f.root.fields().Lookup(ctx, f.root.Client(), f.dataset.Name, name)
	if err != nil {
		return &FieldDir{root: f.root, dataset: f.dataset, field: name}, nil
	}
	if !found {
		return nil, os.ErrNotExist
	}
	return &FieldDir{root: f.root, dataset: f.dataset, field: field.Name}, nil
}

type FieldDir struct {
	root    *Root
	dataset *axiomclient.Dataset
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
	dataset *axiomclient.Dataset
	format  string
}

func (d *DatasetSchemaFile) buildSchema(ctx context.Context) ([]byte, error) {
	fields, err := d.root.fields().List(ctx, d.root.Client(), d.dataset.Name)
	if err != nil {
		return nil, err
	}
	switch d.format {
	case "json":
		data, err := json.MarshalIndent(fields, "", "  ")
		if err != nil {
			return nil, err
		}
		return append(data, '\n'), nil
	case "csv":
		return fieldsToCSV(fields)
	default:
		return nil, os.ErrInvalid
	}
}

func fieldsToCSV(fields []axiomclient.Field) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{"name", "type", "description", "unit"}); err != nil {
		return nil, err
	}
	for _, f := range fields {
		if f.Hidden {
			continue
		}
		if err := w.Write([]string{f.Name, f.Type, f.Description, f.Unit}); err != nil {
			return nil, err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *DatasetSchemaFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return DynamicFileInfo("schema." + d.format), nil
}

func (d *DatasetSchemaFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data, err := d.buildSchema(ctx)
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}

type DatasetSampleFile struct {
	root    *Root
	dataset *axiomclient.Dataset
}

func (d *DatasetSampleFile) buildSample(ctx context.Context) ([]byte, error) {
	cfg := d.root.Config()
	apl := "['" + d.dataset.Name + "']\n| take " + strconv.Itoa(cfg.SampleLimit)
	return d.root.Executor().ExecuteAPL(ctx, apl, "ndjson", query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: true,
		EnsureLimit:     false,
	})
}

func (d *DatasetSampleFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return DynamicFileInfo("sample.ndjson"), nil
}

func (d *DatasetSampleFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data, err := d.buildSample(ctx)
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}

type FieldQueryFile struct {
	root    *Root
	dataset *axiomclient.Dataset
	field   string
	kind    string
}

func (f *FieldQueryFile) buildFieldQuery(ctx context.Context) ([]byte, error) {
	var expr string
	switch f.kind {
	case "top":
		expr = "summarize topk(" + f.field + ", 10)"
	case "histogram":
		expr = "summarize histogram(" + f.field + ", 100)"
	default:
		return nil, os.ErrInvalid
	}
	apl := "['" + f.dataset.Name + "']\n| " + expr
	return f.root.Executor().ExecuteAPL(ctx, apl, "csv", query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: true,
		EnsureLimit:     false,
	})
}

func (f *FieldQueryFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return DynamicFileInfo(f.kind + ".csv"), nil
}

func (f *FieldQueryFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data, err := f.buildFieldQuery(ctx)
	if err != nil {
		// Return error as file content so users can see why the query failed
		return newBytesFile([]byte("error: " + err.Error() + "\n")), nil
	}
	return newBytesFile(data), nil
}
