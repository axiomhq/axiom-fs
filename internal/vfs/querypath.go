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

func (q *QueryPathResultFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("result.ndjson", 0), nil
}

func (q *QueryPathResultFile) Open(ctx context.Context, flags int) (billy.File, error) {
	compiled, err := compilePath(q.dataset, q.segments, q.root.Config())
	if err != nil {
		return nil, err
	}
	result, err := q.root.Executor().ExecuteAPLResult(ctx, compiled.APL, compiled.Format, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
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

func (q *QueryPathErrorFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo("result.error", 0), nil
}

func (q *QueryPathErrorFile) Open(ctx context.Context, flags int) (billy.File, error) {
	compiled, err := compilePath(q.dataset, q.segments, q.root.Config())
	if err != nil {
		data := query.BuildErrorAPL("", err)
		return newBytesFile(data), nil
	}
	_, err = q.root.Executor().ExecuteAPL(ctx, compiled.APL, compiled.Format, query.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	data := query.BuildErrorAPL(compiled.APL, err)
	return newBytesFile(data), nil
}
