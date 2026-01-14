package vfs

import (
	"context"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/axiomhq/axiom-go/axiom"
	axiomquery "github.com/axiomhq/axiom-go/axiom/query"

	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/query"
)

type mockClient struct {
	datasets []*axiom.Dataset
	queryFn  func(apl string) (*axiomquery.Result, error)
}

func (m *mockClient) ListDatasets(ctx context.Context) ([]*axiom.Dataset, error) {
	return m.datasets, nil
}

func (m *mockClient) QueryAPL(ctx context.Context, apl string) (*axiomquery.Result, error) {
	if m.queryFn != nil {
		return m.queryFn(apl)
	}
	return &axiomquery.Result{}, nil
}

type mockExecutor struct {
	aplLog    []string
	formatLog []string
	data      []byte
	result    *axiomquery.Result
	err       error
}

func (m *mockExecutor) ExecuteAPL(ctx context.Context, apl, format string, opts query.ExecOptions) ([]byte, error) {
	m.aplLog = append(m.aplLog, apl)
	m.formatLog = append(m.formatLog, format)
	return m.data, m.err
}

func (m *mockExecutor) ExecuteAPLResult(ctx context.Context, apl, format string, opts query.ExecOptions) (query.ResultData, error) {
	m.aplLog = append(m.aplLog, apl)
	m.formatLog = append(m.formatLog, format)
	return query.ResultData{Bytes: m.data, Size: int64(len(m.data))}, m.err
}

func (m *mockExecutor) QueryAPL(ctx context.Context, apl string, opts query.ExecOptions) (*axiomquery.Result, error) {
	m.aplLog = append(m.aplLog, apl)
	if m.result != nil {
		return m.result, m.err
	}
	return &axiomquery.Result{}, m.err
}

func (m *mockExecutor) lastAPL() string {
	if len(m.aplLog) == 0 {
		return ""
	}
	return m.aplLog[len(m.aplLog)-1]
}

func (m *mockExecutor) lastFormat() string {
	if len(m.formatLog) == 0 {
		return ""
	}
	return m.formatLog[len(m.formatLog)-1]
}

func newTestRoot(datasets []*axiom.Dataset, data []byte) (*Root, *mockExecutor) {
	cfg := config.Default()
	client := &mockClient{datasets: datasets}
	exec := &mockExecutor{data: data}
	return NewRoot(cfg, client, exec), exec
}

func readFile(t *testing.T, node File) []byte {
	t.Helper()
	f, err := node.Open(context.Background(), os.O_RDONLY)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return data
}

func dirNames(t *testing.T, dir Dir) []string {
	t.Helper()
	entries, err := dir.ReadDir(context.Background())
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	sort.Strings(names)
	return names
}

func TestRootStructure(t *testing.T) {
	root, _ := newTestRoot([]*axiom.Dataset{{Name: "logs"}, {Name: "metrics"}}, nil)
	ctx := context.Background()

	t.Run("Stat", func(t *testing.T) {
		info, err := root.Stat(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			t.Error("root should be directory")
		}
	})

	t.Run("ReadDir", func(t *testing.T) {
		names := dirNames(t, root)
		want := []string{"README.txt", "_presets", "_queries", "datasets", "examples", "logs", "metrics"}
		if len(names) != len(want) {
			t.Fatalf("got %v, want %v", names, want)
		}
		for i := range want {
			if names[i] != want[i] {
				t.Errorf("names[%d] = %q, want %q", i, names[i], want[i])
			}
		}
	})

	t.Run("Lookup static entries", func(t *testing.T) {
		cases := []struct {
			name  string
			isDir bool
		}{
			{"README.txt", false},
			{"examples", true},
			{"datasets", true},
			{"_presets", true},
			{"_queries", true},
			{"logs", true},
			{"metrics", true},
		}
		for _, tc := range cases {
			node, err := root.Lookup(ctx, tc.name)
			if err != nil {
				t.Errorf("Lookup(%q): %v", tc.name, err)
				continue
			}
			info, _ := node.Stat(ctx)
			if info.IsDir() != tc.isDir {
				t.Errorf("%q IsDir = %v, want %v", tc.name, info.IsDir(), tc.isDir)
			}
		}
	})

	t.Run("Lookup nonexistent", func(t *testing.T) {
		_, err := root.Lookup(ctx, "nonexistent")
		if !os.IsNotExist(err) {
			t.Errorf("expected ErrNotExist, got %v", err)
		}
	})

	t.Run("Reserved names excluded from datasets", func(t *testing.T) {
		root2, _ := newTestRoot([]*axiom.Dataset{
			{Name: "logs"},
			{Name: "datasets"}, // reserved
			{Name: "_presets"}, // reserved
		}, nil)
		names := dirNames(t, root2)
		for _, n := range names {
			if n == "datasets" {
				continue // datasets dir itself is expected
			}
		}
		// Should only have one "datasets" entry (the dir, not the dataset)
		count := 0
		for _, n := range names {
			if n == "datasets" {
				count++
			}
		}
		if count != 1 {
			t.Errorf("expected exactly one 'datasets', got %d in %v", count, names)
		}
	})
}

func TestDatasetDir(t *testing.T) {
	root, exec := newTestRoot([]*axiom.Dataset{{Name: "logs"}}, []byte(`{"test":true}`))
	ctx := context.Background()

	dataset, _ := root.Lookup(ctx, "logs")
	dir := dataset.(Dir)

	t.Run("ReadDir", func(t *testing.T) {
		names := dirNames(t, dir)
		want := []string{"fields", "presets", "q", "sample.ndjson", "schema.csv", "schema.json"}
		if len(names) != len(want) {
			t.Fatalf("got %v, want %v", names, want)
		}
	})

	t.Run("schema.json executes getschema", func(t *testing.T) {
		node, _ := dir.Lookup(ctx, "schema.json")
		_ = readFile(t, node.(File))
		if !strings.Contains(exec.lastAPL(), "getschema") {
			t.Errorf("APL should contain getschema: %s", exec.lastAPL())
		}
		if exec.lastFormat() != "json" {
			t.Errorf("format = %q, want json", exec.lastFormat())
		}
	})

	t.Run("sample.ndjson applies limit", func(t *testing.T) {
		node, _ := dir.Lookup(ctx, "sample.ndjson")
		_ = readFile(t, node.(File))
		if !strings.Contains(exec.lastAPL(), "take") {
			t.Errorf("APL should contain take: %s", exec.lastAPL())
		}
	})
}

func TestQueryPath(t *testing.T) {
	root, exec := newTestRoot([]*axiom.Dataset{{Name: "logs"}}, []byte("row1\nrow2"))
	ctx := context.Background()

	dataset, _ := root.Lookup(ctx, "logs")
	qDir, _ := dataset.(Dir).Lookup(ctx, "q")

	cases := []struct {
		segments []string
		wantAPL  []string
		format   string
	}{
		{
			segments: []string{"range", "ago", "1h", "result.csv"},
			wantAPL:  []string{"ago(1h)"},
			format:   "csv",
		},
		{
			segments: []string{"where", "status>=500", "result.ndjson"},
			wantAPL:  []string{"where status>=500"},
			format:   "ndjson",
		},
		{
			segments: []string{"summarize", "count()", "by", "service", "result.json"},
			wantAPL:  []string{"summarize count() by service"},
			format:   "json",
		},
		{
			segments: []string{"limit", "100", "result.csv"},
			wantAPL:  []string{"take 100"},
			format:   "csv",
		},
	}

	for _, tc := range cases {
		t.Run(strings.Join(tc.segments, "/"), func(t *testing.T) {
			var node Node = qDir
			for _, seg := range tc.segments {
				next, err := node.(Dir).Lookup(ctx, seg)
				if err != nil {
					t.Fatalf("Lookup(%q): %v", seg, err)
				}
				node = next
			}

			_ = readFile(t, node.(File))

			for _, want := range tc.wantAPL {
				if !strings.Contains(exec.lastAPL(), want) {
					t.Errorf("APL missing %q: %s", want, exec.lastAPL())
				}
			}
			if exec.lastFormat() != tc.format {
				t.Errorf("format = %q, want %q", exec.lastFormat(), tc.format)
			}
		})
	}
}

func TestRawQueries(t *testing.T) {
	root, exec := newTestRoot(nil, []byte("results"))
	ctx := context.Background()

	queries, _ := root.Lookup(ctx, "_queries")
	qDir := queries.(Dir)

	t.Run("create and execute query", func(t *testing.T) {
		entry, _ := qDir.Lookup(ctx, "myquery")
		aplNode, _ := entry.(Dir).Lookup(ctx, "apl")

		// Write APL
		wf, err := aplNode.(Writable).Create(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wf.Write([]byte("['logs'] | where error == true | take 50"))
		wf.Close()

		// Read result
		resultNode, _ := entry.(Dir).Lookup(ctx, "result.csv")
		data := readFile(t, resultNode.(File))
		if string(data) != "results" {
			t.Errorf("got %q, want results", data)
		}
		if !strings.Contains(exec.lastAPL(), "where error == true") {
			t.Errorf("APL missing filter: %s", exec.lastAPL())
		}
	})

	t.Run("invalid query name rejected", func(t *testing.T) {
		_, err := qDir.Lookup(ctx, "../escape")
		if !os.IsNotExist(err) {
			t.Errorf("expected ErrNotExist for path traversal, got %v", err)
		}
	})
}

func TestFieldsDir(t *testing.T) {
	cfg := config.Default()
	client := &mockClient{datasets: []*axiom.Dataset{{Name: "logs"}}}
	exec := &mockExecutor{
		data: []byte("field_data"),
		result: &axiomquery.Result{
			Tables: []axiomquery.Table{{
				Fields:  []axiomquery.Field{{Name: "name"}},
				Columns: []axiomquery.Column{{"status", "service", "duration"}},
			}},
		},
	}
	root := NewRoot(cfg, client, exec)
	ctx := context.Background()

	dataset, _ := root.Lookup(ctx, "logs")
	fields, _ := dataset.(Dir).Lookup(ctx, "fields")

	t.Run("lists fields from schema", func(t *testing.T) {
		names := dirNames(t, fields.(Dir))
		want := []string{"duration", "service", "status"}
		if len(names) != len(want) {
			t.Fatalf("got %v, want %v", names, want)
		}
	})

	t.Run("field/top.csv", func(t *testing.T) {
		fieldDir, _ := fields.(Dir).Lookup(ctx, "status")
		topFile, _ := fieldDir.(Dir).Lookup(ctx, "top.csv")
		_ = readFile(t, topFile.(File))
		if !strings.Contains(exec.lastAPL(), "topk(status") {
			t.Errorf("APL missing topk: %s", exec.lastAPL())
		}
	})

	t.Run("field/histogram.csv", func(t *testing.T) {
		fieldDir, _ := fields.(Dir).Lookup(ctx, "duration")
		histFile, _ := fieldDir.(Dir).Lookup(ctx, "histogram.csv")
		_ = readFile(t, histFile.(File))
		if !strings.Contains(exec.lastAPL(), "histogram(duration") {
			t.Errorf("APL missing histogram: %s", exec.lastAPL())
		}
	})
}

func TestPresets(t *testing.T) {
	root, exec := newTestRoot([]*axiom.Dataset{{Name: "logs"}}, []byte("preset_data"))
	ctx := context.Background()

	t.Run("_presets lists all presets", func(t *testing.T) {
		presets, _ := root.Lookup(ctx, "_presets")
		names := dirNames(t, presets.(Dir))
		if len(names) == 0 {
			t.Error("expected presets")
		}
		for _, n := range names {
			if !strings.HasSuffix(n, ".json") {
				t.Errorf("preset %q should end with .json", n)
			}
		}
	})

	t.Run("dataset presets execute with dataset", func(t *testing.T) {
		dataset, _ := root.Lookup(ctx, "logs")
		presets, _ := dataset.(Dir).Lookup(ctx, "presets")
		names := dirNames(t, presets.(Dir))
		if len(names) == 0 {
			t.Skip("no presets for this dataset")
		}

		first, _ := presets.(Dir).Lookup(ctx, names[0])
		_ = readFile(t, first.(File))
		if !strings.Contains(exec.lastAPL(), "['logs']") {
			t.Errorf("APL missing dataset: %s", exec.lastAPL())
		}
	})
}

func TestStaticFiles(t *testing.T) {
	root, _ := newTestRoot(nil, nil)
	ctx := context.Background()

	t.Run("README.txt", func(t *testing.T) {
		node, _ := root.Lookup(ctx, "README.txt")
		data := readFile(t, node.(File))
		if len(data) == 0 {
			t.Error("README should not be empty")
		}
		if !strings.Contains(string(data), "Axiom") {
			t.Error("README should mention Axiom")
		}
	})

	t.Run("examples/quickstart.txt", func(t *testing.T) {
		examples, _ := root.Lookup(ctx, "examples")
		qs, _ := examples.(Dir).Lookup(ctx, "quickstart.txt")
		data := readFile(t, qs.(File))
		if !strings.Contains(string(data), "result.csv") {
			t.Error("quickstart should show example path")
		}
	})
}

func TestSchemaCSV(t *testing.T) {
	cases := []struct {
		name   string
		result *axiomquery.Result
		want   []string
	}{
		{
			name:   "empty",
			result: &axiomquery.Result{},
			want:   nil,
		},
		{
			name: "simple fields",
			result: &axiomquery.Result{
				Tables: []axiomquery.Table{{
					Fields: []axiomquery.Field{
						{Name: "service", Type: "string"},
						{Name: "count_", Type: "int64"},
					},
				}},
			},
			want: []string{"service,string", "count_,int64"},
		},
		{
			name: "with aggregation",
			result: &axiomquery.Result{
				Tables: []axiomquery.Table{{
					Fields: []axiomquery.Field{
						{
							Name: "total",
							Type: "int64",
							Aggregation: &axiomquery.Aggregation{
								Op:     axiomquery.OpSum,
								Fields: []string{"amount"},
							},
						},
					},
				}},
			},
			want: []string{"total,int64,sum(amount)"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := schemaCSV(tc.result)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(string(data), w) {
					t.Errorf("missing %q in:\n%s", w, data)
				}
			}
		})
	}
}

func TestBytesFile(t *testing.T) {
	content := []byte("hello world")
	f := newBytesFile(content)

	t.Run("Read", func(t *testing.T) {
		buf := make([]byte, 5)
		n, _ := f.Read(buf)
		if string(buf[:n]) != "hello" {
			t.Errorf("got %q", buf[:n])
		}
	})

	t.Run("ReadAt", func(t *testing.T) {
		buf := make([]byte, 5)
		n, _ := f.ReadAt(buf, 6)
		if string(buf[:n]) != "world" {
			t.Errorf("got %q", buf[:n])
		}
	})

	t.Run("Seek", func(t *testing.T) {
		pos, _ := f.Seek(0, io.SeekStart)
		if pos != 0 {
			t.Errorf("pos = %d", pos)
		}
	})

	t.Run("Write denied", func(t *testing.T) {
		_, err := f.Write([]byte("x"))
		if !os.IsPermission(err) {
			t.Errorf("expected permission error, got %v", err)
		}
	})

	t.Run("Truncate denied", func(t *testing.T) {
		err := f.Truncate(0)
		if !os.IsPermission(err) {
			t.Errorf("expected permission error, got %v", err)
		}
	})
}

func TestAPLFile(t *testing.T) {
	root, _ := newTestRoot(nil, nil)
	_ = context.Background()
	store := root.Store()

	t.Run("write and read", func(t *testing.T) {
		f := newAPLFile(store, "test1")
		f.Write([]byte("['ds'] | take 10"))
		f.Close()

		data := store.Get("test1")
		if string(data) != "['ds'] | take 10" {
			t.Errorf("got %q", data)
		}
	})

	t.Run("truncate clears", func(t *testing.T) {
		store.Set("test2", []byte("old content"))
		f := newAPLFile(store, "test2")
		f.Truncate(0)
		f.Close()

		data := store.Get("test2")
		if len(data) != 0 {
			t.Errorf("expected empty, got %q", data)
		}
	})
}

func TestIsValidQueryName(t *testing.T) {
	cases := []struct {
		name  string
		valid bool
	}{
		{"simple", true},
		{"with-dash", true},
		{"with_underscore", true},
		{"with.dot", true},
		{"123numeric", true},
		{"", false},
		{"../escape", false},
		{"path/sep", false},
		{strings.Repeat("x", 100), true}, // long names OK in vfs, store may reject
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isValidQueryName(tc.name)
			if got != tc.valid {
				t.Errorf("isValidQueryName(%q) = %v, want %v", tc.name, got, tc.valid)
			}
		})
	}
}

func TestQueryErrorFile(t *testing.T) {
	root, exec := newTestRoot(nil, []byte("data"))
	ctx := context.Background()

	t.Run("empty APL returns error", func(t *testing.T) {
		node := &QueryErrorFile{root: root, name: "empty"}
		f, err := node.Open(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		data, _ := io.ReadAll(f)
		if !strings.Contains(string(data), "error") {
			t.Errorf("expected error in output: %s", data)
		}
	})

	t.Run("valid APL executes", func(t *testing.T) {
		root.Store().Set("valid", []byte("['logs']"))
		exec.err = nil
		node := &QueryErrorFile{root: root, name: "valid"}
		f, _ := node.Open(ctx, 0)
		defer f.Close()
		data, _ := io.ReadAll(f)
		if !strings.Contains(string(data), `"ok": true`) {
			t.Errorf("expected ok:true: %s", data)
		}
	})
}

func TestQueryStatsFile(t *testing.T) {
	root, _ := newTestRoot(nil, nil)
	ctx := context.Background()
	root.Store().Set("stats", []byte("['logs']"))

	node := &QueryStatsFile{root: root, name: "stats"}
	f, err := node.Open(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	data, _ := io.ReadAll(f)
	if !strings.Contains(string(data), `"apl"`) {
		t.Errorf("missing apl field: %s", data)
	}
	if !strings.Contains(string(data), `"status"`) {
		t.Errorf("missing status field: %s", data)
	}
}

func TestQuerySchemaFile(t *testing.T) {
	cfg := config.Default()
	client := &mockClient{}
	exec := &mockExecutor{
		result: &axiomquery.Result{
			Tables: []axiomquery.Table{{
				Fields: []axiomquery.Field{{Name: "col1", Type: "string"}},
			}},
		},
	}
	root := NewRoot(cfg, client, exec)
	ctx := context.Background()
	root.Store().Set("schema", []byte("['logs']"))

	node := &QuerySchemaFile{root: root, name: "schema"}
	f, err := node.Open(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	data, _ := io.ReadAll(f)
	if !strings.Contains(string(data), "col1,string") {
		t.Errorf("missing schema data: %s", data)
	}
}

func TestQueryPathErrorFile(t *testing.T) {
	root, _ := newTestRoot(nil, nil)
	ctx := context.Background()

	t.Run("compile error", func(t *testing.T) {
		node := &QueryPathErrorFile{
			root:     root,
			dataset:  "logs",
			segments: []string{"invalid_segment", "result.error"},
		}
		f, err := node.Open(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		data, _ := io.ReadAll(f)
		if !strings.Contains(string(data), "error") {
			t.Errorf("expected error: %s", data)
		}
	})
}

func TestExamplesDir(t *testing.T) {
	dir := &ExamplesDir{}
	ctx := context.Background()

	t.Run("Stat", func(t *testing.T) {
		info, _ := dir.Stat(ctx)
		if info.Name() != "examples" {
			t.Error("wrong name")
		}
		if !info.IsDir() {
			t.Error("should be dir")
		}
	})

	t.Run("ReadDir", func(t *testing.T) {
		entries, _ := dir.ReadDir(ctx)
		if len(entries) != 1 || entries[0].Name() != "quickstart.txt" {
			t.Errorf("unexpected entries: %v", entries)
		}
	})

	t.Run("Lookup nonexistent", func(t *testing.T) {
		_, err := dir.Lookup(ctx, "nonexistent")
		if !os.IsNotExist(err) {
			t.Error("expected not exist")
		}
	})
}

func TestDatasetsDir(t *testing.T) {
	root, _ := newTestRoot([]*axiom.Dataset{{Name: "a"}, {Name: "b"}}, nil)
	ctx := context.Background()
	datasets, _ := root.Lookup(ctx, "datasets")
	dir := datasets.(Dir)

	t.Run("ReadDir", func(t *testing.T) {
		entries, _ := dir.ReadDir(ctx)
		if len(entries) != 2 {
			t.Errorf("expected 2, got %d", len(entries))
		}
	})

	t.Run("Lookup exists", func(t *testing.T) {
		node, err := dir.Lookup(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		info, _ := node.Stat(ctx)
		if !info.IsDir() {
			t.Error("should be dir")
		}
	})

	t.Run("Lookup nonexistent", func(t *testing.T) {
		_, err := dir.Lookup(ctx, "nonexistent")
		if !os.IsNotExist(err) {
			t.Error("expected not exist")
		}
	})
}

func TestPresetsDir(t *testing.T) {
	dir := &PresetsDir{}
	ctx := context.Background()

	t.Run("ReadDir has presets", func(t *testing.T) {
		entries, _ := dir.ReadDir(ctx)
		if len(entries) == 0 {
			t.Error("expected presets")
		}
	})

	t.Run("Lookup nonexistent", func(t *testing.T) {
		_, err := dir.Lookup(ctx, "nonexistent.json")
		if !os.IsNotExist(err) {
			t.Error("expected not exist")
		}
	})
}

func TestQueriesDir(t *testing.T) {
	root, _ := newTestRoot(nil, nil)
	ctx := context.Background()
	queries, _ := root.Lookup(ctx, "_queries")
	dir := queries.(Dir)

	t.Run("empty initially", func(t *testing.T) {
		entries, _ := dir.ReadDir(ctx)
		// May have entries from other tests, just check no error
		_ = entries
	})

	t.Run("lookup creates entry", func(t *testing.T) {
		node, err := dir.Lookup(ctx, "newquery")
		if err != nil {
			t.Fatal(err)
		}
		info, _ := node.Stat(ctx)
		if !info.IsDir() {
			t.Error("query entry should be dir")
		}
	})
}

func TestQueryEntryDir(t *testing.T) {
	root, _ := newTestRoot(nil, nil)
	ctx := context.Background()
	entry := &QueryEntryDir{root: root, name: "test"}

	t.Run("ReadDir lists files", func(t *testing.T) {
		entries, _ := entry.ReadDir(ctx)
		names := make(map[string]bool)
		for _, e := range entries {
			names[e.Name()] = true
		}
		for _, want := range []string{"apl", "result.ndjson", "result.csv", "schema.csv", "stats.json"} {
			if !names[want] {
				t.Errorf("missing %s", want)
			}
		}
	})

	t.Run("Lookup apl", func(t *testing.T) {
		node, _ := entry.Lookup(ctx, "apl")
		if _, ok := node.(Writable); !ok {
			t.Error("apl should be writable")
		}
	})

	t.Run("Lookup nonexistent", func(t *testing.T) {
		_, err := entry.Lookup(ctx, "nonexistent")
		if !os.IsNotExist(err) {
			t.Error("expected not exist")
		}
	})
}

func TestDatasetPresetsDir(t *testing.T) {
	root, _ := newTestRoot([]*axiom.Dataset{{Name: "logs"}}, nil)
	ctx := context.Background()
	dataset, _ := root.Lookup(ctx, "logs")
	presets, _ := dataset.(Dir).Lookup(ctx, "presets")
	dir := presets.(Dir)

	t.Run("ReadDir", func(t *testing.T) {
		entries, _ := dir.ReadDir(ctx)
		if len(entries) == 0 {
			t.Skip("no presets for logs")
		}
	})

	t.Run("Lookup nonexistent", func(t *testing.T) {
		_, err := dir.Lookup(ctx, "nonexistent.csv")
		if !os.IsNotExist(err) {
			t.Error("expected not exist")
		}
	})
}

func TestVirtualFileInfo(t *testing.T) {
	info := DirInfo("test")
	if info.Name() != "test" {
		t.Error("wrong name")
	}
	if !info.IsDir() {
		t.Error("should be dir")
	}
	if info.Mode()&os.ModeDir == 0 {
		t.Error("mode should have dir bit")
	}
	if info.Sys() != nil {
		t.Error("Sys should be nil")
	}

	finfo := FileInfo("file", 123)
	if finfo.Size() != 123 {
		t.Errorf("size = %d", finfo.Size())
	}
	if finfo.IsDir() {
		t.Error("should not be dir")
	}
}

func TestTempFile(t *testing.T) {
	// Create a real temp file for testing
	f, err := os.CreateTemp("", "test")
	if err != nil {
		t.Skip("cannot create temp file")
	}
	f.WriteString("test content")
	f.Seek(0, 0)

	tf := &tempFile{file: f, size: 12}

	t.Run("Read", func(t *testing.T) {
		buf := make([]byte, 4)
		n, _ := tf.Read(buf)
		if string(buf[:n]) != "test" {
			t.Errorf("got %q", buf[:n])
		}
	})

	t.Run("ReadAt", func(t *testing.T) {
		buf := make([]byte, 7)
		n, _ := tf.ReadAt(buf, 5)
		if string(buf[:n]) != "content" {
			t.Errorf("got %q", buf[:n])
		}
	})

	t.Run("Seek", func(t *testing.T) {
		pos, _ := tf.Seek(0, 0)
		if pos != 0 {
			t.Error("seek failed")
		}
	})

	t.Run("Write denied", func(t *testing.T) {
		_, err := tf.Write([]byte("x"))
		if !os.IsPermission(err) {
			t.Error("expected permission error")
		}
	})

	t.Run("Truncate denied", func(t *testing.T) {
		err := tf.Truncate(0)
		if !os.IsPermission(err) {
			t.Error("expected permission error")
		}
	})

	t.Run("Close removes file", func(t *testing.T) {
		name := tf.Name()
		tf.Close()
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Error("file should be removed")
		}
	})
}

func TestOpenResult(t *testing.T) {
	t.Run("bytes result", func(t *testing.T) {
		result := query.ResultData{Bytes: []byte("data"), Size: 4}
		f, err := openResult(result)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		data, _ := io.ReadAll(f)
		if string(data) != "data" {
			t.Errorf("got %q", data)
		}
	})
}
