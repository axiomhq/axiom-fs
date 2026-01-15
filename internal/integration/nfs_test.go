package integration

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"

	"github.com/axiomhq/axiom-fs/internal/cache"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/nfsfs"
	"github.com/axiomhq/axiom-fs/internal/query"
	"github.com/axiomhq/axiom-fs/internal/vfs"
)

// TestNFS_EndToEnd tests the full NFS mount and operations.
//
// Requirements:
//   - AXIOM_FS_TEST_DATASET must be set
//   - On Linux: root access (run with: sudo -E go test ...)
//   - On macOS: no root required
//
// Example:
//
//	AXIOM_FS_TEST_DATASET=http-logs go test -v ./internal/integration -run TestNFS_EndToEnd
func TestNFS_EndToEnd(t *testing.T) {
	if os.Getuid() != 0 && !isDarwin() {
		t.Skip("skipping: requires root on Linux (run with sudo -E)")
	}
	skipIfNotConfigured(t)

	// Start NFS server on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	t.Logf("NFS server on port %d", port)

	// Create the filesystem
	cfg := config.Default()
	client := newClient(t)
	c := cache.New(cfg.CacheTTL, cfg.MaxCacheEntries, cfg.MaxCacheBytes, cfg.CacheDir)
	executor := query.NewExecutor(client, c, cfg.DefaultRange, cfg.DefaultLimit, cfg.MaxCacheBytes, cfg.MaxInMemoryBytes, cfg.TempDir)
	root := vfs.NewRoot(cfg, client, executor)
	billyFS := nfsfs.New(root)

	handler := nfshelper.NewNullAuthHandler(billyFS)
	cacheHandler := nfshelper.NewCachingHandler(handler, 1024)

	// Start NFS server
	go func() {
		_ = nfs.Serve(listener, cacheHandler)
	}()

	// Create mount point
	mountPoint, err := os.MkdirTemp("", "axiom-fs-test-*")
	if err != nil {
		t.Fatalf("failed to create mount point: %v", err)
	}
	defer os.RemoveAll(mountPoint)

	// Mount with timeout
	portStr := fmt.Sprintf("%d", port)
	mountCmd := exec.Command("mount", "-t", "nfs",
		"-o", "vers=3,tcp,port="+portStr+",mountport="+portStr+",timeo=50,retrans=2",
		"127.0.0.1:/", mountPoint)
	if out, err := mountCmd.CombinedOutput(); err != nil {
		t.Fatalf("mount failed: %v\n%s", err, out)
	}
	t.Logf("mounted at %s", mountPoint)

	// Ensure unmount on cleanup
	defer func() {
		cmd := exec.Command("umount", "-f", mountPoint)
		cmd.Run()
	}()

	// Wait for mount to stabilize
	time.Sleep(200 * time.Millisecond)

	t.Run("ListRoot", func(t *testing.T) {
		entries, err := os.ReadDir(mountPoint)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(entries) == 0 {
			t.Fatal("root directory is empty")
		}

		names := make(map[string]bool)
		for _, e := range entries {
			names[e.Name()] = true
		}
		if !names["datasets"] {
			t.Error("missing 'datasets'")
		}
		if !names["README.txt"] {
			t.Error("missing README.txt")
		}
	})

	t.Run("ReadREADME", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(mountPoint, "README.txt"))
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if !strings.Contains(string(data), "Axiom") {
			t.Error("README should contain 'Axiom'")
		}
	})

	t.Run("WriteReadAPL", func(t *testing.T) {
		aplPath := filepath.Join(mountPoint, "_queries", "nfs-e2e", "apl")
		apl := "['" + testDataset + "'] | where _time > ago(1h) | project _time | take 2"

		if err := os.WriteFile(aplPath, []byte(apl), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		data, err := os.ReadFile(aplPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(data) != apl {
			t.Errorf("APL mismatch: got %q", string(data))
		}
	})

	t.Run("QueryResults", func(t *testing.T) {
		resultPath := filepath.Join(mountPoint, "_queries", "nfs-e2e", "result.csv")
		data, err := os.ReadFile(resultPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if !strings.Contains(string(data), "_time") {
			t.Errorf("expected _time in result, got: %s", string(data))
		}
	})

	t.Run("QueryPath", func(t *testing.T) {
		queryPath := filepath.Join(mountPoint, testDataset, "q", "range", "ago", "1h", "summarize", "count()", "result.csv")
		data, err := os.ReadFile(queryPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if !strings.Contains(string(data), "count_") {
			t.Errorf("expected count_ in result, got: %s", string(data))
		}
	})

	t.Run("SchemaCSV", func(t *testing.T) {
		schemaPath := filepath.Join(mountPoint, testDataset, "schema.csv")
		data, err := os.ReadFile(schemaPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if len(data) == 0 {
			t.Error("schema.csv is empty")
		}
	})
}

func isDarwin() bool {
	return os.Getenv("GOOS") == "darwin" || (os.Getenv("GOOS") == "" && runtime.GOOS == "darwin")
}
