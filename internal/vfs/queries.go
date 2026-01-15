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

func (q *QueryResultFile) execute(ctx context.Context) (query.ResultData, error) {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		return query.ResultData{}, err
	}
	return q.root.Executor().ExecuteAPLResult(ctx, apl, q.format, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false, // Raw APL queries run as-is
		EnsureLimit:     false,
	})
}

func (q *QueryResultFile) Stat(ctx context.Context) (os.FileInfo, error) {
	result, err := q.execute(ctx)
	if err != nil {
		return DynamicFileInfo("result." + q.format), nil
	}
	return FileInfo("result."+q.format, result.Size), nil
}

func (q *QueryResultFile) Open(ctx context.Context, flags int) (billy.File, error) {
	result, err := q.execute(ctx)
	if err != nil {
		return nil, err
	}
	return openResult(result)
}

type QueryErrorFile struct {
	root *Root
	name string
}

func (q *QueryErrorFile) buildError(ctx context.Context) []byte {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		return query.BuildErrorAPL(apl, err)
	}
	_, err := q.root.Executor().ExecuteAPL(ctx, apl, "ndjson", query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	return query.BuildErrorAPL(apl, err)
}

func (q *QueryErrorFile) Stat(ctx context.Context) (os.FileInfo, error) {
	data := q.buildError(ctx)
	return FileInfo("result.error", int64(len(data))), nil
}

func (q *QueryErrorFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data := q.buildError(ctx)
	return newBytesFile(data), nil
}

type QuerySchemaFile struct {
	root *Root
	name string
}

func (q *QuerySchemaFile) buildSchema(ctx context.Context) ([]byte, error) {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		return nil, err
	}
	result, err := q.root.Executor().QueryAPL(ctx, apl, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, err
	}
	return schemaCSV(result)
}

func (q *QuerySchemaFile) Stat(ctx context.Context) (os.FileInfo, error) {
	data, err := q.buildSchema(ctx)
	if err != nil {
		return DynamicFileInfo("schema.csv"), nil
	}
	return FileInfo("schema.csv", int64(len(data))), nil
}

func (q *QuerySchemaFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data, err := q.buildSchema(ctx)
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}

type QueryStatsFile struct {
	root *Root
	name string
}

func (q *QueryStatsFile) buildStats(ctx context.Context) ([]byte, error) {
	apl := string(q.root.Store().Get(q.name))
	if err := query.ValidateAPL(apl); err != nil {
		return nil, err
	}
	result, err := q.root.Executor().QueryAPL(ctx, apl, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
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
	return append(data, '\n'), nil
}

func (q *QueryStatsFile) Stat(ctx context.Context) (os.FileInfo, error) {
	data, err := q.buildStats(ctx)
	if err != nil {
		return DynamicFileInfo("stats.json"), nil
	}
	return FileInfo("stats.json", int64(len(data))), nil
}

func (q *QueryStatsFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data, err := q.buildStats(ctx)
	if err != nil {
		return nil, err
	}
	return newBytesFile(data), nil
}
