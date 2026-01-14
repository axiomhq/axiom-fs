package vfs

import (
	"context"
	"encoding/json"
	"os"
	"sort"

	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/query"
)

type QueriesDir struct {
	root *Root
}

func (q *QueriesDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("_queries"), nil
}

func (q *QueriesDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	names := q.root.Store().Names()
	entries := make([]os.FileInfo, 0, len(names))
	for _, name := range names {
		entries = append(entries, DirInfo(name))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (q *QueriesDir) Lookup(ctx context.Context, name string) (Node, error) {
	if !isValidQueryName(name) {
		return nil, os.ErrNotExist
	}
	return &QueryEntryDir{root: q.root, name: name}, nil
}

type QueryEntryDir struct {
	root *Root
	name string
}

func (q *QueryEntryDir) Stat(ctx context.Context) (os.FileInfo, error) {
	if !isValidQueryName(q.name) {
		return nil, os.ErrNotExist
	}
	return DirInfo(q.name), nil
}

func (q *QueryEntryDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	aplData := q.root.Store().Get(q.name)
	return []os.FileInfo{
		WritableFileInfo("apl", int64(len(aplData))),
		FileInfo("result.ndjson", 0),
		FileInfo("result.csv", 0),
		FileInfo("result.json", 0),
		FileInfo("result.error", 0),
		FileInfo("schema.csv", 0),
		FileInfo("stats.json", 0),
	}, nil
}

func (q *QueryEntryDir) Lookup(ctx context.Context, name string) (Node, error) {
	if !isValidQueryName(q.name) {
		return nil, os.ErrNotExist
	}
	switch name {
	case "apl":
		return &APLFile{root: q.root, name: q.name}, nil
	case "result.ndjson":
		return &QueryResultFile{root: q.root, name: q.name, format: "ndjson"}, nil
	case "result.csv":
		return &QueryResultFile{root: q.root, name: q.name, format: "csv"}, nil
	case "result.json":
		return &QueryResultFile{root: q.root, name: q.name, format: "json"}, nil
	case "result.error":
		return &QueryErrorFile{root: q.root, name: q.name}, nil
	case "schema.csv":
		return &QuerySchemaFile{root: q.root, name: q.name}, nil
	case "stats.json":
		return &QueryStatsFile{root: q.root, name: q.name}, nil
	default:
		return nil, os.ErrNotExist
	}
}

type APLFile struct {
	root *Root
	name string
}

func (a *APLFile) Stat(ctx context.Context) (os.FileInfo, error) {
	data := a.root.Store().Get(a.name)
	return WritableFileInfo("apl", int64(len(data))), nil
}

func (a *APLFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data := a.root.Store().Get(a.name)
	return newBytesFile(data), nil
}

func (a *APLFile) Create(ctx context.Context) (billy.File, error) {
	return newAPLFile(a.root.Store(), a.name), nil
}

type QueryResultFile struct {
	root   *Root
	name   string
	format string
}

func (q *QueryResultFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("result."+q.format, 0), nil
}

func (q *QueryResultFile) Open(ctx context.Context, flags int) (billy.File, error) {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		return nil, err
	}
	result, err := q.root.Executor().ExecuteAPLResult(ctx, apl, q.format, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, err
	}
	return openResult(result)
}

type QueryErrorFile struct {
	root *Root
	name string
}

func (q *QueryErrorFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("result.error", 0), nil
}

func (q *QueryErrorFile) Open(ctx context.Context, flags int) (billy.File, error) {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		data := query.BuildErrorAPL(apl, err)
		return newBytesFile(data), nil
	}
	_, err := q.root.Executor().ExecuteAPL(ctx, apl, "ndjson", query.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	data := query.BuildErrorAPL(apl, err)
	return newBytesFile(data), nil
}

type QuerySchemaFile struct {
	root *Root
	name string
}

func (q *QuerySchemaFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("schema.csv", 0), nil
}

func (q *QuerySchemaFile) Open(ctx context.Context, flags int) (billy.File, error) {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		return nil, err
	}
	result, err := q.root.Executor().QueryAPL(ctx, apl, query.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, err
	}
	data, err := schemaCSV(result)
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}

type QueryStatsFile struct {
	root *Root
	name string
}

func (q *QueryStatsFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("stats.json", 0), nil
}

func (q *QueryStatsFile) Open(ctx context.Context, flags int) (billy.File, error) {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		return nil, err
	}
	result, err := q.root.Executor().QueryAPL(ctx, apl, query.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, err
	}
	payload := map[string]any{
		"apl":    apl,
		"status": result.Status,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return nil, err
	}
	return newBytesFile(append(data, '\n')), nil
}
