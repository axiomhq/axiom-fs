package vfs

import (
	"context"
	"os"
	"strings"

	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/query"
)

type QueryPathDir struct {
	root     *Root
	dataset  string
	segments []string
}

func (q *QueryPathDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("q"), nil
}

func (q *QueryPathDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	return []os.FileInfo{}, nil
}

func (q *QueryPathDir) Lookup(ctx context.Context, name string) (Node, error) {
	if strings.HasPrefix(name, "result.") {
		ext := strings.TrimPrefix(name, "result.")
		if ext == "error" {
			return &QueryPathErrorFile{root: q.root, dataset: q.dataset, segments: append(q.segments, name)}, nil
		}
		return &QueryPathResultFile{root: q.root, dataset: q.dataset, segments: append(q.segments, name)}, nil
	}
	return &QueryPathDir{root: q.root, dataset: q.dataset, segments: append(q.segments, name)}, nil
}

type QueryPathResultFile struct {
	root     *Root
	dataset  string
	segments []string
}

func (q *QueryPathResultFile) execute(ctx context.Context) (query.ResultData, error) {
	compiled, err := compilePath(q.dataset, q.segments, q.root.Config())
	if err != nil {
		return query.ResultData{}, err
	}
	return q.root.Executor().ExecuteAPLResult(ctx, compiled.APL, compiled.Format, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
}

func (q *QueryPathResultFile) Stat(ctx context.Context) (os.FileInfo, error) {
	// Execute query to get accurate size - results are cached by executor
	result, err := q.execute(ctx)
	if err != nil {
		// Return placeholder on error - Open will return the actual error
		return DynamicFileInfo("result.ndjson"), nil
	}
	return FileInfo("result.ndjson", result.Size), nil
}

func (q *QueryPathResultFile) Open(ctx context.Context, flags int) (billy.File, error) {
	result, err := q.execute(ctx)
	if err != nil {
		return nil, err
	}
	return openResult(result)
}

type QueryPathErrorFile struct {
	root     *Root
	dataset  string
	segments []string
}

func (q *QueryPathErrorFile) buildError(ctx context.Context) []byte {
	compiled, err := compilePath(q.dataset, q.segments, q.root.Config())
	if err != nil {
		return query.BuildErrorAPL("", err)
	}
	_, err = q.root.Executor().ExecuteAPL(ctx, compiled.APL, compiled.Format, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	return query.BuildErrorAPL(compiled.APL, err)
}

func (q *QueryPathErrorFile) Stat(ctx context.Context) (os.FileInfo, error) {
	data := q.buildError(ctx)
	return FileInfo("result.error", int64(len(data))), nil
}

func (q *QueryPathErrorFile) Open(ctx context.Context, flags int) (billy.File, error) {
	data := q.buildError(ctx)
	return newBytesFile(data), nil
}
