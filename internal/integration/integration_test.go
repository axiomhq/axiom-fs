// Package integration provides integration tests for axiom-fs.
//
// These tests require real Axiom credentials and a dataset with data.
// Configure with environment variables:
//
//	AXIOM_FS_TEST_URL      - Axiom API URL (optional, defaults to ~/.axiom.toml)
//	AXIOM_FS_TEST_TOKEN    - Axiom API token (optional, defaults to ~/.axiom.toml)
//	AXIOM_FS_TEST_ORG_ID   - Axiom org ID (optional, defaults to ~/.axiom.toml)
//	AXIOM_FS_TEST_DATASET  - Dataset name with recent data (required)
//
// Example:
//
//	AXIOM_FS_TEST_DATASET=http-logs go test -v ./internal/integration/...
package integration

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
	"github.com/axiomhq/axiom-fs/internal/cache"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/nfsfs"
	"github.com/axiomhq/axiom-fs/internal/query"
	"github.com/axiomhq/axiom-fs/internal/vfs"
)

var (
	testDataset string
	testURL     string
	testToken   string
	testOrgID   string
)

func init() {
	testDataset = os.Getenv("AXIOM_FS_TEST_DATASET")
	testURL = os.Getenv("AXIOM_FS_TEST_URL")
	testToken = os.Getenv("AXIOM_FS_TEST_TOKEN")
	testOrgID = os.Getenv("AXIOM_FS_TEST_ORG_ID")
}

func skipIfNotConfigured(t *testing.T) {
	t.Helper()
	if testDataset == "" {
		t.Skip("skipping: AXIOM_FS_TEST_DATASET not set")
	}
}

func newClient(t *testing.T) *axiomclient.Client {
	t.Helper()
	client, err := axiomclient.NewWithEnvOverrides(testURL, testToken, testOrgID)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client
}

func newTestFS(t *testing.T) *nfsfs.FS {
	t.Helper()
	cfg := config.Default()
	client := newClient(t)
	c := cache.New(cfg.CacheTTL, cfg.MaxCacheEntries, cfg.MaxCacheBytes, cfg.CacheDir)
	exec := query.NewExecutor(client, c, cfg.DefaultRange, cfg.DefaultLimit, cfg.MaxCacheBytes, cfg.MaxInMemoryBytes, cfg.TempDir)
	root := vfs.NewRoot(cfg, client, exec)
	return nfsfs.New(root)
}

func TestIntegration_ListDatasets(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	entries, err := fs.ReadDir("/datasets")
	if err != nil {
		t.Fatalf("ReadDir /datasets: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one dataset")
	}

	// Verify test dataset exists
	found := false
	for _, e := range entries {
		if e.Name() == testDataset {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("test dataset %q not found in datasets list", testDataset)
	}
}

func TestIntegration_RootContents(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatalf("ReadDir /: %v", err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name()] = true
	}

	required := []string{"datasets", "README.txt", "_queries", "_presets"}
	for _, name := range required {
		if !names[name] {
			t.Errorf("missing required entry: %s", name)
		}
	}
}

func TestIntegration_ReadREADME(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	f, err := fs.Open("/README.txt")
	if err != nil {
		t.Fatalf("Open README.txt: %v", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("README.txt is empty")
	}
	if !strings.Contains(string(data), "Axiom") {
		t.Error("README.txt should mention Axiom")
	}
}

func TestIntegration_DatasetStructure(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	entries, err := fs.ReadDir("/" + testDataset)
	if err != nil {
		t.Fatalf("ReadDir /%s: %v", testDataset, err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name()] = true
	}

	required := []string{"schema.json", "schema.csv", "sample.ndjson", "fields", "presets", "q"}
	for _, name := range required {
		if !names[name] {
			t.Errorf("dataset %s missing: %s", testDataset, name)
		}
	}
}

func TestIntegration_SchemaCSV(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	f, err := fs.Open("/" + testDataset + "/schema.csv")
	if err != nil {
		t.Fatalf("Open schema.csv: %v", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("Read schema: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("schema.csv is empty")
	}

	// Schema should have headers
	content := string(data)
	if !strings.Contains(content, "ColumnName") && !strings.Contains(content, "_time") {
		t.Errorf("schema.csv should have column info, got: %s", content[:min(100, len(content))])
	}
}

func TestIntegration_RawAPLQuery(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	queryName := "test-" + time.Now().Format("20060102150405")
	aplPath := "/_queries/" + queryName + "/apl"
	resultPath := "/_queries/" + queryName + "/result.csv"

	// APL with time range and project for speed
	apl := "['" + testDataset + "'] | where _time > ago(1h) | project _time | take 5"

	// Write APL
	aplFile, err := fs.Create(aplPath)
	if err != nil {
		t.Fatalf("Create APL file: %v", err)
	}
	if _, err := aplFile.Write([]byte(apl)); err != nil {
		t.Fatalf("Write APL: %v", err)
	}
	if err := aplFile.Close(); err != nil {
		t.Fatalf("Close APL file: %v", err)
	}

	// Read back APL to verify write
	aplFile2, err := fs.Open(aplPath)
	if err != nil {
		t.Fatalf("Open APL file: %v", err)
	}
	aplData, _ := io.ReadAll(aplFile2)
	aplFile2.Close()
	if string(aplData) != apl {
		t.Fatalf("APL mismatch: got %q, want %q", string(aplData), apl)
	}

	// Read result
	resultFile, err := fs.Open(resultPath)
	if err != nil {
		t.Fatalf("Open result: %v", err)
	}
	resultData, err := io.ReadAll(resultFile)
	resultFile.Close()
	if err != nil {
		t.Fatalf("Read result: %v", err)
	}

	// Result should have _time header
	if !strings.Contains(string(resultData), "_time") {
		t.Errorf("expected _time in result, got: %s", string(resultData))
	}
}

func TestIntegration_QueryPath(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	// Count aggregation - always returns data if dataset has any
	queryPath := "/" + testDataset + "/q/range/ago/1h/summarize/count()/result.csv"

	f, err := fs.Open(queryPath)
	if err != nil {
		t.Fatalf("Open query path: %v", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("Read query result: %v", err)
	}

	// Should have count_ in the result
	if !strings.Contains(string(data), "count_") {
		t.Errorf("expected count_ in aggregation result, got: %s", string(data))
	}
}

func TestIntegration_FileSizeAccuracy(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	queryPath := "/" + testDataset + "/q/range/ago/1h/project/_time/limit/3/result.csv"

	// Stat BEFORE Open - this is how NFS clients behave (GETATTR before READ)
	// The fix ensures Stat executes the query to get accurate size
	info, err := fs.Stat(queryPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	statedSize := info.Size()

	// Size must not be the 64MB placeholder
	if statedSize == 64*1024*1024 {
		t.Fatalf("Stat returned placeholder size (64MB), expected actual query result size")
	}

	// Now open and read to verify stated size matches actual content
	f, err := fs.Open(queryPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	data, _ := io.ReadAll(f)
	f.Close()

	actualSize := int64(len(data))
	if statedSize != actualSize {
		t.Errorf("size mismatch: stat=%d, actual=%d", statedSize, actualSize)
	}
}

func TestIntegration_FieldHistogramError(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	// Find a string field to test histogram on (which should fail)
	// histogram() only works on numeric fields
	stringField := os.Getenv("AXIOM_FS_TEST_STRING_FIELD")
	if stringField == "" {
		stringField = "data.message" // common field name
	}

	histogramPath := "/" + testDataset + "/fields/" + stringField + "/histogram.csv"

	f, err := fs.Open(histogramPath)
	if err != nil {
		// This is the expected behavior - Open should return an error
		t.Logf("Open correctly returned error: %v", err)
		return
	}
	defer f.Close()

	// If Open succeeded, check if we got error content or empty file
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// The file should NOT be empty - it should either:
	// 1. Not open at all (error from Open), or
	// 2. Contain an error message
	if len(data) == 0 {
		t.Fatal("histogram.csv is empty but should contain an error message for string fields")
	}

	t.Logf("histogram.csv content: %s", string(data))
}

func TestIntegration_WriteAndReadAPL(t *testing.T) {
	skipIfNotConfigured(t)
	fs := newTestFS(t)

	queryName := "write-" + time.Now().Format("20060102150405")
	aplPath := "/_queries/" + queryName + "/apl"

	testAPL := "['test'] | take 1"

	// Write
	f, err := fs.Create(aplPath)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	n, err := f.Write([]byte(testAPL))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(testAPL) {
		t.Errorf("wrote %d bytes, expected %d", n, len(testAPL))
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back
	f2, err := fs.Open(aplPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	data, err := io.ReadAll(f2)
	f2.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if string(data) != testAPL {
		t.Errorf("read %q, expected %q", string(data), testAPL)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
