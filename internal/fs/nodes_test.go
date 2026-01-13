package fs

import (
	"context"
	"io"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/axiomhq/axiom-go/axiom"
	axiomquery "github.com/axiomhq/axiom-go/axiom/query"
	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/presets"
	queryexec "github.com/axiomhq/axiom-fs/internal/query"
)

type fakeClient struct {
	datasets []*axiom.Dataset
}

func (f *fakeClient) ListDatasets(ctx context.Context) ([]*axiom.Dataset, error) {
	return f.datasets, nil
}

func (f *fakeClient) QueryAPL(ctx context.Context, apl string) (*axiomquery.Result, error) {
	return &axiomquery.Result{}, nil
}

type fakeExecutor struct {
	lastAPL    string
	lastFormat string
	lastOpts   queryexec.ExecOptions
	data       []byte
	result     *axiomquery.Result
}

func (f *fakeExecutor) ExecuteAPL(ctx context.Context, apl, format string, opts queryexec.ExecOptions) ([]byte, error) {
	f.lastAPL = apl
	f.lastFormat = format
	f.lastOpts = opts
	return f.data, nil
}

func (f *fakeExecutor) ExecuteAPLResult(ctx context.Context, apl, format string, opts queryexec.ExecOptions) (queryexec.ResultData, error) {
	f.lastAPL = apl
	f.lastFormat = format
	f.lastOpts = opts
	return queryexec.ResultData{Bytes: f.data, Size: int64(len(f.data))}, nil
}

func (f *fakeExecutor) QueryAPL(ctx context.Context, apl string, opts queryexec.ExecOptions) (*axiomquery.Result, error) {
	f.lastAPL = apl
	f.lastOpts = opts
	if f.result == nil {
		return &axiomquery.Result{}, nil
	}
	return f.result, nil
}

func openData(t *testing.T, node interface {
	Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno)
}) []byte {
	t.Helper()
	handle, _, errno := node.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open errno: %d", errno)
	}
	switch h := handle.(type) {
	case *dataHandle:
		return h.data
	case *fileHandle:
		data := make([]byte, h.size)
		n, err := h.file.ReadAt(data, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}
		_ = h.Release(context.Background())
		return data[:n]
	default:
		t.Fatalf("handle type = %T, want *dataHandle or *fileHandle", handle)
		return nil
	}
}

func TestRootReaddir(t *testing.T) {
	cfg := config.Default()
	client := &fakeClient{datasets: []*axiom.Dataset{
		{Name: "logs"},
		{Name: "metrics"},
	}}
	exec := &fakeExecutor{data: []byte("ok")}

	fsys := New(cfg, client, exec)
	root := &Root{fsys: fsys}

	stream, errno := root.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir errno: %d", errno)
	}
	var names []string
	for stream.HasNext() {
		entry, errNo := stream.Next()
		if errNo != 0 {
			t.Fatalf("Next errno: %d", errNo)
		}
		names = append(names, entry.Name)
	}
	stream.Close()

	sort.Strings(names)
	want := []string{"README.txt", "_presets", "_queries", "datasets", "examples", "logs", "metrics"}
	if len(names) != len(want) {
		t.Fatalf("names len = %d, want %d", len(names), len(want))
	}
	for i, name := range want {
		if names[i] != name {
			t.Fatalf("names[%d] = %q, want %q", i, names[i], name)
		}
	}
}

func TestQueryPathResult(t *testing.T) {
	cfg := config.Default()
	client := &fakeClient{}
	exec := &fakeExecutor{data: []byte("ok")}

	fsys := New(cfg, client, exec)
	node := &QueryPathResultFile{
		fsys:    fsys,
		dataset: "logs",
		segments: []string{
			"where", "status>=500",
			"result.csv",
		},
	}

	data := openData(t, node)
	if string(data) != "ok" {
		t.Fatalf("data = %q, want ok", string(data))
	}
	if exec.lastFormat != "csv" {
		t.Fatalf("format = %q, want csv", exec.lastFormat)
	}
	if !strings.Contains(exec.lastAPL, "where status>=500") {
		t.Fatalf("APL missing where: %s", exec.lastAPL)
	}
	if !strings.Contains(exec.lastAPL, "where _time between") {
		t.Fatalf("APL missing range: %s", exec.lastAPL)
	}
	if !strings.Contains(exec.lastAPL, "take 10000") {
		t.Fatalf("APL missing default limit: %s", exec.lastAPL)
	}
}

func TestPresetResult(t *testing.T) {
	cfg := config.Default()
	client := &fakeClient{}
	exec := &fakeExecutor{data: []byte("ok")}

	fsys := New(cfg, client, exec)
	preset := presets.DefaultCatalog().Core[0]
	node := &PresetResultFile{
		fsys:    fsys,
		dataset: &axiom.Dataset{Name: "logs"},
		preset:  preset,
	}

	data := openData(t, node)
	if string(data) != "ok" {
		t.Fatalf("data = %q, want ok", string(data))
	}
	if exec.lastFormat != preset.Format {
		t.Fatalf("format = %q, want %q", exec.lastFormat, preset.Format)
	}
	if !strings.Contains(exec.lastAPL, "['logs']") {
		t.Fatalf("APL missing dataset: %s", exec.lastAPL)
	}
	if !exec.lastOpts.EnsureTimeRange || !exec.lastOpts.EnsureLimit {
		t.Fatalf("expected ensure options true, got %+v", exec.lastOpts)
	}
}

func TestRawQueryLifecycle(t *testing.T) {
	cfg := config.Default()
	client := &fakeClient{}
	exec := &fakeExecutor{data: []byte("ok")}
	fsys := New(cfg, client, exec)

	aplFile := &APLFile{fsys: fsys, name: "errors"}
	if _, errno := aplFile.Write(context.Background(), nil, []byte("['logs'] | summarize count()"), 0); errno != 0 {
		t.Fatalf("write errno: %d", errno)
	}

	result := &QueryResultFile{fsys: fsys, name: "errors", format: "ndjson"}
	data := openData(t, result)
	if string(data) != "ok" {
		t.Fatalf("data = %q, want ok", string(data))
	}
	if !strings.Contains(exec.lastAPL, "summarize count()") {
		t.Fatalf("APL missing summarize: %s", exec.lastAPL)
	}
}

func TestSchemaCSV(t *testing.T) {
	result := &axiomquery.Result{
		Tables: []axiomquery.Table{
			{
				Fields: []axiomquery.Field{
					{Name: "service", Type: "string"},
					{
						Name: "count_",
						Type: "int64",
						Aggregation: &axiomquery.Aggregation{
							Op:     axiomquery.OpCount,
							Fields: []string{"*"},
						},
					},
				},
			},
		},
	}
	data, err := schemaCSV(result)
	if err != nil {
		t.Fatalf("schemaCSV error: %v", err)
	}
	if !strings.Contains(string(data), "service,string,") {
		t.Fatalf("schemaCSV missing field: %s", string(data))
	}
	if !strings.Contains(string(data), "count_,int64,count(*)") {
		t.Fatalf("schemaCSV missing aggregation: %s", string(data))
	}
}

func TestFieldQueries(t *testing.T) {
	cfg := config.Default()
	client := &fakeClient{}
	exec := &fakeExecutor{data: []byte("ok")}
	fsys := New(cfg, client, exec)

	fieldTop := &FieldQueryFile{
		fsys:    fsys,
		dataset: &axiom.Dataset{Name: "logs"},
		field:   "service",
		kind:    "top",
	}
	openData(t, fieldTop)
	if !strings.Contains(exec.lastAPL, "summarize topk(service, 10)") {
		t.Fatalf("top query missing: %s", exec.lastAPL)
	}

	fieldHist := &FieldQueryFile{
		fsys:    fsys,
		dataset: &axiom.Dataset{Name: "logs"},
		field:   "duration",
		kind:    "histogram",
	}
	openData(t, fieldHist)
	if !strings.Contains(exec.lastAPL, "summarize histogram(duration, 100)") {
		t.Fatalf("histogram query missing: %s", exec.lastAPL)
	}
}

func TestFieldsListingUsesSchema(t *testing.T) {
	cfg := config.Default()
	client := &fakeClient{}
	exec := &fakeExecutor{
		result: &axiomquery.Result{
			Tables: []axiomquery.Table{
				{
					Fields: []axiomquery.Field{
						{Name: "name"},
						{Name: "type"},
					},
					Columns: []axiomquery.Column{
						{"service", "duration"},
						{"string", "float"},
					},
				},
			},
		},
	}
	fsys := New(cfg, client, exec)
	dir := &FieldsDir{fsys: fsys, dataset: &axiom.Dataset{Name: "logs"}}
	stream, errno := dir.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir errno: %d", errno)
	}
	var names []string
	for stream.HasNext() {
		entry, errNo := stream.Next()
		if errNo != 0 {
			t.Fatalf("Next errno: %d", errNo)
		}
		names = append(names, entry.Name)
	}
	stream.Close()
	if len(names) != 2 {
		t.Fatalf("names len = %d, want 2", len(names))
	}
}
func TestQuerySchemaAndStats(t *testing.T) {
	cfg := config.Default()
	client := &fakeClient{}
	exec := &fakeExecutor{
		result: &axiomquery.Result{
			Status: axiomquery.Status{RowsMatched: 12},
		},
	}
	fsys := New(cfg, client, exec)
	apl := "['logs'] | summarize count()"
	fsys.Store.Set("mine", []byte(apl))

	schema := &QuerySchemaFile{fsys: fsys, name: "mine"}
	openData(t, schema)
	if exec.lastOpts.EnsureTimeRange != true {
		t.Fatalf("expected EnsureTimeRange true, got %+v", exec.lastOpts)
	}

	stats := &QueryStatsFile{fsys: fsys, name: "mine"}
	data := openData(t, stats)
	if !strings.Contains(string(data), "\"rowsMatched\": 12") {
		t.Fatalf("stats missing rowsMatched: %s", string(data))
	}
	if !strings.Contains(string(data), "\"apl\"") {
		t.Fatalf("stats missing apl: %s", string(data))
	}
}
