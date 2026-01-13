package fs

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/axiomhq/axiom-go/axiom"
	axiomquery "github.com/axiomhq/axiom-go/axiom/query"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/axiomhq/axiom-fs/internal/compiler"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/presets"
	queryexec "github.com/axiomhq/axiom-fs/internal/query"
)

const (
	modeDir  = 0o555
	modeFile = 0o444
	modeRW   = 0o644
)

var (
	readmeText = []byte(strings.TrimSpace(`
Axiom FUSE FS

Most useful:
  /<dataset>/presets/*.csv

Advanced:
  /<dataset>/q/<...>/result.ndjson

Raw APL:
  /_queries/<name>/apl
`) + "\n")

	exampleText = []byte(strings.TrimSpace(`
Example query:
/mnt/axiom/logs/q/range/ago/1h/where/status>=500/summarize/count()/by/service/order/count_:desc/limit/50/result.csv
`) + "\n")
)

type Root struct {
	fs.Inode
	fsys *FS
}

func (r *Root) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (r *Root) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		dirEntry("datasets", fuse.S_IFDIR),
		dirEntry("README.txt", fuse.S_IFREG),
		dirEntry("examples", fuse.S_IFDIR),
		dirEntry("_presets", fuse.S_IFDIR),
		dirEntry("_queries", fuse.S_IFDIR),
	}

	datasets, err := r.fsys.datasets.List(ctx, r.fsys.Client)
	if err != nil {
		return nil, errno(err)
	}
	for _, dataset := range datasets {
		if dataset == nil || dataset.Name == "" {
			continue
		}
		if isReservedRoot(dataset.Name) {
			continue
		}
		entries = append(entries, dirEntry(dataset.Name, fuse.S_IFDIR))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return fs.NewListDirStream(entries), 0
}

func (r *Root) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "README.txt":
		return r.newNode(ctx, &ReadFile{data: readmeText, mode: modeFile}, fuse.S_IFREG), 0
	case "examples":
		return r.newNode(ctx, &ExamplesDir{fsys: r.fsys}, fuse.S_IFDIR), 0
	case "datasets":
		return r.newNode(ctx, &DatasetsDir{fsys: r.fsys}, fuse.S_IFDIR), 0
	case "_presets":
		return r.newNode(ctx, &PresetsDir{fsys: r.fsys}, fuse.S_IFDIR), 0
	case "_queries":
		return r.newNode(ctx, &QueriesDir{fsys: r.fsys}, fuse.S_IFDIR), 0
	}

	dataset, err := r.lookupDataset(ctx, name)
	if err != nil {
		return nil, errno(err)
	}
	if dataset == nil {
		return nil, syscall.ENOENT
	}
	return r.newNode(ctx, &DatasetDir{fsys: r.fsys, dataset: dataset}, fuse.S_IFDIR), 0
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

func (r *Root) newNode(ctx context.Context, node fs.InodeEmbedder, mode uint32) *fs.Inode {
	return r.NewInode(ctx, node, fs.StableAttr{Mode: mode})
}

type DatasetsDir struct {
	fs.Inode
	fsys *FS
}

func (d *DatasetsDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (d *DatasetsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	datasets, err := d.fsys.datasets.List(ctx, d.fsys.Client)
	if err != nil {
		return nil, errno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(datasets))
	for _, dataset := range datasets {
		if dataset == nil || dataset.Name == "" {
			continue
		}
		entries = append(entries, dirEntry(dataset.Name, fuse.S_IFDIR))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return fs.NewListDirStream(entries), 0
}

func (d *DatasetsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	for _, dataset := range mustDatasets(ctx, d.fsys) {
		if dataset != nil && dataset.Name == name {
			return d.NewInode(ctx, &DatasetDir{fsys: d.fsys, dataset: dataset}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
		}
	}
	return nil, syscall.ENOENT
}

type ExamplesDir struct {
	fs.Inode
	fsys *FS
}

func (e *ExamplesDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (e *ExamplesDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{dirEntry("quickstart.txt", fuse.S_IFREG)}
	return fs.NewListDirStream(entries), 0
}

func (e *ExamplesDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "quickstart.txt" {
		return e.NewInode(ctx, &ReadFile{data: exampleText, mode: modeFile}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	}
	return nil, syscall.ENOENT
}

type PresetsDir struct {
	fs.Inode
	fsys *FS
}

func (p *PresetsDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (p *PresetsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, 0)
	for _, preset := range allPresets() {
		entries = append(entries, dirEntry(preset.Name+".json", fuse.S_IFREG))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return fs.NewListDirStream(entries), 0
}

func (p *PresetsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	base := strings.TrimSuffix(name, ".json")
	for _, preset := range allPresets() {
		if preset.Name == base {
			data := presets.MetadataJSON(preset)
			return p.NewInode(ctx, &ReadFile{data: data, mode: modeFile}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
		}
	}
	return nil, syscall.ENOENT
}

type QueriesDir struct {
	fs.Inode
	fsys *FS
}

func (q *QueriesDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (q *QueriesDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	names := q.fsys.Store.Names()
	entries := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, dirEntry(name, fuse.S_IFDIR))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return fs.NewListDirStream(entries), 0
}

func (q *QueriesDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !isValidQueryName(name) {
		return nil, syscall.ENOENT
	}
	return q.NewInode(ctx, &QueryEntryDir{fsys: q.fsys, name: name}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
}

type QueryEntryDir struct {
	fs.Inode
	fsys *FS
	name string
}

func (q *QueryEntryDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if !isValidQueryName(q.name) {
		return syscall.ENOENT
	}
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (q *QueryEntryDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		dirEntry("apl", fuse.S_IFREG),
		dirEntry("result.ndjson", fuse.S_IFREG),
		dirEntry("result.csv", fuse.S_IFREG),
		dirEntry("result.json", fuse.S_IFREG),
		dirEntry("result.error", fuse.S_IFREG),
		dirEntry("schema.csv", fuse.S_IFREG),
		dirEntry("stats.json", fuse.S_IFREG),
	}
	return fs.NewListDirStream(entries), 0
}

func (q *QueryEntryDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !isValidQueryName(q.name) {
		return nil, syscall.ENOENT
	}
	switch name {
	case "apl":
		return q.NewInode(ctx, &APLFile{fsys: q.fsys, name: q.name}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "result.ndjson":
		return q.NewInode(ctx, &QueryResultFile{fsys: q.fsys, name: q.name, format: "ndjson"}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "result.csv":
		return q.NewInode(ctx, &QueryResultFile{fsys: q.fsys, name: q.name, format: "csv"}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "result.json":
		return q.NewInode(ctx, &QueryResultFile{fsys: q.fsys, name: q.name, format: "json"}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "result.error":
		return q.NewInode(ctx, &QueryErrorFile{fsys: q.fsys, name: q.name}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "schema.csv":
		return q.NewInode(ctx, &QuerySchemaFile{fsys: q.fsys, name: q.name}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "stats.json":
		return q.NewInode(ctx, &QueryStatsFile{fsys: q.fsys, name: q.name}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	default:
		return nil, syscall.ENOENT
	}
}

type DatasetDir struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
}

func (d *DatasetDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (d *DatasetDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		dirEntry("schema.json", fuse.S_IFREG),
		dirEntry("schema.csv", fuse.S_IFREG),
		dirEntry("sample.ndjson", fuse.S_IFREG),
		dirEntry("fields", fuse.S_IFDIR),
		dirEntry("presets", fuse.S_IFDIR),
		dirEntry("q", fuse.S_IFDIR),
	}
	return fs.NewListDirStream(entries), 0
}

func (d *DatasetDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "schema.json":
		return d.NewInode(ctx, &DatasetSchemaFile{fsys: d.fsys, dataset: d.dataset, format: "json"}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "schema.csv":
		return d.NewInode(ctx, &DatasetSchemaFile{fsys: d.fsys, dataset: d.dataset, format: "csv"}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "sample.ndjson":
		return d.NewInode(ctx, &DatasetSampleFile{fsys: d.fsys, dataset: d.dataset}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "fields":
		return d.NewInode(ctx, &FieldsDir{fsys: d.fsys, dataset: d.dataset}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
	case "presets":
		return d.NewInode(ctx, &DatasetPresetsDir{fsys: d.fsys, dataset: d.dataset}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
	case "q":
		return d.NewInode(ctx, &QueryPathDir{fsys: d.fsys, dataset: d.dataset.Name, segments: nil}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
	default:
		return nil, syscall.ENOENT
	}
}

type FieldsDir struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
}

func (f *FieldsDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (f *FieldsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	fields, err := f.fsys.fields.List(ctx, f.fsys, f.dataset.Name)
	if err != nil {
		return nil, errno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(fields))
	for _, field := range fields {
		entries = append(entries, dirEntry(field, fuse.S_IFDIR))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return fs.NewListDirStream(entries), 0
}

func (f *FieldsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return f.NewInode(ctx, &FieldDir{fsys: f.fsys, dataset: f.dataset, field: name}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
}

type FieldDir struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
	field   string
}

func (f *FieldDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (f *FieldDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		dirEntry("top.csv", fuse.S_IFREG),
		dirEntry("histogram.csv", fuse.S_IFREG),
	}
	return fs.NewListDirStream(entries), 0
}

func (f *FieldDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "top.csv":
		return f.NewInode(ctx, &FieldQueryFile{fsys: f.fsys, dataset: f.dataset, field: f.field, kind: "top"}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	case "histogram.csv":
		return f.NewInode(ctx, &FieldQueryFile{fsys: f.fsys, dataset: f.dataset, field: f.field, kind: "histogram"}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	default:
		return nil, syscall.ENOENT
	}
}

type DatasetPresetsDir struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
}

func (p *DatasetPresetsDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (p *DatasetPresetsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{}
	for _, preset := range presets.PresetsForDataset(p.dataset) {
		filename := preset.Name + "." + preset.Format
		entries = append(entries, dirEntry(filename, fuse.S_IFREG))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return fs.NewListDirStream(entries), 0
}

func (p *DatasetPresetsDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	base := strings.TrimSuffix(name, path.Ext(name))
	ext := strings.TrimPrefix(path.Ext(name), ".")
	for _, preset := range presets.PresetsForDataset(p.dataset) {
		if preset.Name == base && preset.Format == ext {
			return p.NewInode(ctx, &PresetResultFile{fsys: p.fsys, dataset: p.dataset, preset: preset}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
		}
	}
	return nil, syscall.ENOENT
}

type QueryPathDir struct {
	fs.Inode
	fsys     *FS
	dataset  string
	segments []string
}

func (q *QueryPathDir) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | modeDir
	return 0
}

func (q *QueryPathDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

func (q *QueryPathDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if strings.HasPrefix(name, "result.") {
		ext := strings.TrimPrefix(name, "result.")
		if ext == "error" {
			return q.NewInode(ctx, &QueryPathErrorFile{fsys: q.fsys, dataset: q.dataset, segments: append(q.segments, name)}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
		}
		return q.NewInode(ctx, &QueryPathResultFile{fsys: q.fsys, dataset: q.dataset, segments: append(q.segments, name)}, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	}
	return q.NewInode(ctx, &QueryPathDir{fsys: q.fsys, dataset: q.dataset, segments: append(q.segments, name)}, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
}

type ReadFile struct {
	fs.Inode
	data []byte
	mode uint32
}

func (r *ReadFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | r.mode
	out.Size = uint64(len(r.data))
	out.Mtime = uint64(time.Now().Unix())
	return 0
}

func (r *ReadFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return &dataHandle{data: r.data}, fuse.FOPEN_DIRECT_IO, 0
}

type dataHandle struct {
	data []byte
}

func (h *dataHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(h.data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := int(off) + len(dest)
	if end > len(h.data) {
		end = len(h.data)
	}
	return fuse.ReadResultData(h.data[int(off):end]), 0
}

type fileHandle struct {
	file *os.File
	size int64
}

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= h.size {
		return fuse.ReadResultData(nil), 0
	}
	n, err := h.file.ReadAt(dest, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *fileHandle) Release(ctx context.Context) syscall.Errno {
	name := h.file.Name()
	_ = h.file.Close()
	_ = os.Remove(name)
	return 0
}

type APLFile struct {
	fs.Inode
	fsys *FS
	name string
}

func (a *APLFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	data := a.fsys.Store.Get(a.name)
	out.Mode = fuse.S_IFREG | modeRW
	out.Size = uint64(len(data))
	return 0
}

func (a *APLFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	data := a.fsys.Store.Get(a.name)
	return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
}

func (a *APLFile) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	current := a.fsys.Store.Get(a.name)
	needed := int(off) + len(data)
	if needed < 0 {
		return 0, syscall.EINVAL
	}
	if needed > len(current) {
		expanded := make([]byte, needed)
		copy(expanded, current)
		current = expanded
	}
	copy(current[int(off):], data)
	a.fsys.Store.Set(a.name, current)
	return uint32(len(data)), 0
}

func (a *APLFile) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if in.Valid&fuse.FATTR_SIZE != 0 {
		a.fsys.Store.Truncate(a.name)
	}
	return 0
}

type QueryResultFile struct {
	fs.Inode
	fsys   *FS
	name   string
	format string
}

func (q *QueryResultFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (q *QueryResultFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	apl := string(q.fsys.Store.Get(q.name))
	if err := queryexec.ValidateAPL(apl); err != nil {
		return nil, 0, errno(err)
	}
	handle, err := openResult(ctx, q.fsys.Executor, apl, q.format, queryexec.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	return handle, fuse.FOPEN_DIRECT_IO, 0
}

type QueryErrorFile struct {
	fs.Inode
	fsys *FS
	name string
}

func (q *QueryErrorFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (q *QueryErrorFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	apl := string(q.fsys.Store.Get(q.name))
	if err := queryexec.ValidateAPL(apl); err != nil {
		data := queryexec.BuildErrorAPL(apl, err)
		return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
	}
	_, err := q.fsys.Executor.ExecuteAPL(ctx, apl, "ndjson", queryexec.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	data := queryexec.BuildErrorAPL(apl, err)
	return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
}

type QuerySchemaFile struct {
	fs.Inode
	fsys *FS
	name string
}

func (q *QuerySchemaFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (q *QuerySchemaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	apl := string(q.fsys.Store.Get(q.name))
	if err := queryexec.ValidateAPL(apl); err != nil {
		return nil, 0, errno(err)
	}
	result, err := q.fsys.Executor.QueryAPL(ctx, apl, queryexec.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	data, err := schemaCSV(result)
	if err != nil {
		return nil, 0, errno(err)
	}
	return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
}

type QueryStatsFile struct {
	fs.Inode
	fsys *FS
	name string
}

func (q *QueryStatsFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (q *QueryStatsFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	apl := string(q.fsys.Store.Get(q.name))
	if err := queryexec.ValidateAPL(apl); err != nil {
		return nil, 0, errno(err)
	}
	result, err := q.fsys.Executor.QueryAPL(ctx, apl, queryexec.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	payload := map[string]any{
		"apl":    apl,
		"status": result.Status,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return nil, 0, errno(err)
	}
	return &dataHandle{data: append(data, '\n')}, fuse.FOPEN_DIRECT_IO, 0
}

type DatasetSchemaFile struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
	format  string
}

func (d *DatasetSchemaFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (d *DatasetSchemaFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| getschema",
		d.dataset.Name,
		d.fsys.Config.DefaultRange,
	)
	data, err := d.fsys.Executor.ExecuteAPL(ctx, apl, d.format, queryexec.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
}

type DatasetSampleFile struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
}

func (d *DatasetSampleFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (d *DatasetSampleFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| take %d",
		d.dataset.Name,
		d.fsys.Config.DefaultRange,
		d.fsys.Config.SampleLimit,
	)
	data, err := d.fsys.Executor.ExecuteAPL(ctx, apl, "ndjson", queryexec.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
}

type FieldQueryFile struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
	field   string
	kind    string
}

func (f *FieldQueryFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (f *FieldQueryFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	var expr string
	switch f.kind {
	case "top":
		expr = fmt.Sprintf("summarize topk(%s, 10)", f.field)
	case "histogram":
		expr = fmt.Sprintf("summarize histogram(%s, 100)", f.field)
	default:
		return nil, 0, syscall.EINVAL
	}
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| %s",
		f.dataset.Name,
		f.fsys.Config.DefaultRange,
		expr,
	)
	data, err := f.fsys.Executor.ExecuteAPL(ctx, apl, "csv", queryexec.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
}

type PresetResultFile struct {
	fs.Inode
	fsys    *FS
	dataset *axiom.Dataset
	preset  presets.Preset
}

func (p *PresetResultFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (p *PresetResultFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	apl := presets.Render(p.preset, p.dataset.Name, p.fsys.Config.DefaultRange)
	handle, err := openResult(ctx, p.fsys.Executor, apl, p.preset.Format, queryexec.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	return handle, fuse.FOPEN_DIRECT_IO, 0
}

type QueryPathResultFile struct {
	fs.Inode
	fsys     *FS
	dataset  string
	segments []string
}

func (q *QueryPathResultFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (q *QueryPathResultFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	compiled, err := compilePath(q.dataset, q.segments, q.fsys.Config)
	if err != nil {
		return nil, 0, errno(err)
	}
	handle, err := openResult(ctx, q.fsys.Executor, compiled.APL, compiled.Format, queryexec.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	if err != nil {
		return nil, 0, errno(err)
	}
	return handle, fuse.FOPEN_DIRECT_IO, 0
}

type QueryPathErrorFile struct {
	fs.Inode
	fsys     *FS
	dataset  string
	segments []string
}

func (q *QueryPathErrorFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | modeFile
	return 0
}

func (q *QueryPathErrorFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	compiled, err := compilePath(q.dataset, q.segments, q.fsys.Config)
	if err != nil {
		data := queryexec.BuildErrorAPL("", err)
		return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
	}
	_, err = q.fsys.Executor.ExecuteAPL(ctx, compiled.APL, compiled.Format, queryexec.ExecOptions{
		UseCache:        false,
		EnsureTimeRange: false,
		EnsureLimit:     false,
	})
	data := queryexec.BuildErrorAPL(compiled.APL, err)
	return &dataHandle{data: data}, fuse.FOPEN_DIRECT_IO, 0
}

func compilePath(dataset string, segments []string, cfg config.Config) (compiler.Query, error) {
	if len(segments) > 0 && segments[len(segments)-1] == "result.error" {
		segments = append([]string{}, segments[:len(segments)-1]...)
		segments = append(segments, "result.ndjson")
	}
	opts := compiler.Options{
		DefaultRange: cfg.DefaultRange,
		DefaultLimit: cfg.DefaultLimit,
		MaxRange:     cfg.MaxRange,
		MaxLimit:     cfg.MaxLimit,
	}
	return compiler.CompileSegments(dataset, segments, opts)
}

func openResult(ctx context.Context, runner queryexec.Runner, apl, format string, opts queryexec.ExecOptions) (fs.FileHandle, error) {
	result, err := runner.ExecuteAPLResult(ctx, apl, format, opts)
	if err != nil {
		return nil, err
	}
	if result.File != nil {
		return &fileHandle{file: result.File, size: result.Size}, nil
	}
	return &dataHandle{data: result.Bytes}, nil
}

func fetchFields(ctx context.Context, fsys *FS, dataset string) ([]string, error) {
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| getschema",
		dataset,
		fsys.Config.DefaultRange,
	)
	result, err := fsys.Executor.QueryAPL(ctx, apl, queryexec.ExecOptions{})
	if err != nil {
		return nil, err
	}
	return extractFieldNames(result), nil
}

func extractFieldNames(result *axiomquery.Result) []string {
	if result == nil || len(result.Tables) == 0 {
		return nil
	}
	table := result.Tables[0]
	if len(table.Columns) == 0 {
		return nil
	}
	index := -1
	for i, field := range table.Fields {
		switch strings.ToLower(field.Name) {
		case "name", "field", "column", "key":
			index = i
		}
	}
	if index == -1 {
		index = 0
	}
	if index >= len(table.Columns) {
		return nil
	}
	column := table.Columns[index]
	unique := make(map[string]struct{})
	for _, value := range column {
		name := strings.TrimSpace(fmt.Sprint(value))
		if name == "" {
			continue
		}
		unique[name] = struct{}{}
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func allPresets() []presets.Preset {
	catalog := presets.DefaultCatalog()
	list := append([]presets.Preset{}, catalog.Core...)
	list = append(list, catalog.OTel...)
	list = append(list, catalog.Stripe...)
	list = append(list, catalog.Segment...)
	return list
}

func schemaCSV(result *axiomquery.Result) ([]byte, error) {
	if len(result.Tables) == 0 {
		return []byte{}, nil
	}
	fields := result.Tables[0].Fields
	var buf strings.Builder
	writer := csv.NewWriter(&buf)
	if err := writer.Write([]string{"name", "type", "aggregation"}); err != nil {
		return nil, err
	}
	for _, field := range fields {
		agg := ""
		if field.Aggregation != nil {
			agg = aggregationString(field.Aggregation)
		}
		if err := writer.Write([]string{field.Name, field.Type, agg}); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return []byte(buf.String()), nil
}

func aggregationString(agg *axiomquery.Aggregation) string {
	if agg == nil {
		return ""
	}
	op := agg.Op.String()
	if len(agg.Fields) == 0 && len(agg.Args) == 0 {
		return op
	}
	args := append([]string{}, agg.Fields...)
	for _, arg := range agg.Args {
		args = append(args, fmt.Sprint(arg))
	}
	return fmt.Sprintf("%s(%s)", op, strings.Join(args, ", "))
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, syscall.ENOENT) {
		return syscall.ENOENT
	}
	if errors.Is(err, syscall.EINVAL) {
		return syscall.EINVAL
	}
	return syscall.EIO
}

func dirEntry(name string, mode uint32) fuse.DirEntry {
	return fuse.DirEntry{Name: name, Mode: mode}
}

func isReservedRoot(name string) bool {
	switch name {
	case "datasets", "README.txt", "examples", "_presets", "_queries":
		return true
	default:
		return false
	}
}

func isValidQueryName(name string) bool {
	if name == "" {
		return false
	}
	if strings.Contains(name, "/") || strings.Contains(name, string(os.PathSeparator)) {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	return true
}

func mustDatasets(ctx context.Context, fsys *FS) []*axiom.Dataset {
	datasets, err := fsys.datasets.List(ctx, fsys.Client)
	if err != nil {
		return nil
	}
	return datasets
}
