package nfsfs

import (
	"context"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/query"
	"github.com/axiomhq/axiom-fs/internal/vfs"
)

type mockClient struct {
	datasets []axiomclient.Dataset
}

func (m *mockClient) ListDatasets(ctx context.Context) ([]axiomclient.Dataset, error) {
	return m.datasets, nil
}

func (m *mockClient) ListFields(ctx context.Context, datasetID string) ([]axiomclient.Field, error) {
	return []axiomclient.Field{
		{Name: "_time", Type: "datetime"},
		{Name: "message", Type: "string"},
	}, nil
}

func (m *mockClient) QueryAPL(ctx context.Context, apl string) (*axiomclient.QueryResult, error) {
	return &axiomclient.QueryResult{}, nil
}

type mockExecutor struct {
	data []byte
}

func (m *mockExecutor) ExecuteAPL(ctx context.Context, apl, format string, opts query.ExecOptions) ([]byte, error) {
	return m.data, nil
}

func (m *mockExecutor) ExecuteAPLResult(ctx context.Context, apl, format string, opts query.ExecOptions) (query.ResultData, error) {
	return query.ResultData{Bytes: m.data, Size: int64(len(m.data))}, nil
}

func (m *mockExecutor) QueryAPL(ctx context.Context, apl string, opts query.ExecOptions) (*axiomclient.QueryResult, error) {
	return &axiomclient.QueryResult{}, nil
}

func newTestFS(t *testing.T) billy.Filesystem {
	t.Helper()
	cfg := config.Default()
	cfg.CacheDir = t.TempDir()
	client := &mockClient{datasets: []axiomclient.Dataset{{Name: "logs"}, {Name: "metrics"}}}
	exec := &mockExecutor{data: []byte("test_data")}
	root := vfs.NewRoot(cfg, client, exec)
	return New(root)
}

func TestResolve(t *testing.T) {
	fs := newTestFS(t)

	cases := []struct {
		path    string
		wantErr bool
		isDir   bool
	}{
		{"/", false, true},
		{".", false, true},
		{"/datasets", false, true},
		{"/logs", false, true},
		{"/logs/q", false, true},
		{"/README.txt", false, false},
		{"/nonexistent", true, false},
		{"/logs/nonexistent", true, false},
		{"datasets", false, true},          // relative
		{"logs/schema.json", false, false}, // relative nested
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			info, err := fs.Stat(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if info.IsDir() != tc.isDir {
				t.Errorf("IsDir = %v, want %v", info.IsDir(), tc.isDir)
			}
		})
	}
}

func TestPathNormalization(t *testing.T) {
	fs := newTestFS(t)

	// All these should resolve to the same thing
	paths := []string{
		"/datasets",
		"datasets",
		"/datasets/",
		"./datasets",
		"/./datasets",
		"/datasets/.",
	}

	var firstName string
	for _, p := range paths {
		info, err := fs.Stat(p)
		if err != nil {
			t.Errorf("Stat(%q): %v", p, err)
			continue
		}
		if firstName == "" {
			firstName = info.Name()
		}
		if info.Name() != firstName {
			t.Errorf("Stat(%q).Name() = %q, want %q", p, info.Name(), firstName)
		}
	}
}

func TestOpen(t *testing.T) {
	fs := newTestFS(t)

	t.Run("read file", func(t *testing.T) {
		f, err := fs.Open("/README.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		data, _ := io.ReadAll(f)
		if len(data) == 0 {
			t.Error("expected content")
		}
	})

	t.Run("open directory fails", func(t *testing.T) {
		_, err := fs.Open("/datasets")
		if err == nil {
			t.Error("expected error opening directory")
		}
	})

	t.Run("query result file", func(t *testing.T) {
		// sample.ndjson uses the executor, schema.json now uses /fields API
		f, err := fs.Open("/logs/sample.ndjson")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		data, _ := io.ReadAll(f)
		if string(data) != "test_data" {
			t.Errorf("got %q", data)
		}
	})
}

func TestOpenFile(t *testing.T) {
	fs := newTestFS(t)

	t.Run("O_RDONLY succeeds", func(t *testing.T) {
		f, err := fs.OpenFile("/README.txt", os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	})

	t.Run("O_WRONLY on read-only fails", func(t *testing.T) {
		_, err := fs.OpenFile("/README.txt", os.O_WRONLY, 0)
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})

	t.Run("O_RDWR on _queries succeeds", func(t *testing.T) {
		f, err := fs.OpenFile("/_queries/test/apl", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		f.Write([]byte("['logs']"))
		f.Close()
	})

	t.Run("O_RDWR outside _queries fails", func(t *testing.T) {
		_, err := fs.OpenFile("/logs/schema.json", os.O_RDWR, 0)
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})
}

func TestCreate(t *testing.T) {
	fs := newTestFS(t)

	t.Run("in _queries succeeds", func(t *testing.T) {
		f, err := fs.Create("/_queries/newquery/apl")
		if err != nil {
			t.Fatal(err)
		}
		f.Write([]byte("['test']"))
		f.Close()
	})

	t.Run("outside _queries fails", func(t *testing.T) {
		_, err := fs.Create("/logs/newfile.txt")
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})
}

func TestReadDir(t *testing.T) {
	fs := newTestFS(t)

	t.Run("root", func(t *testing.T) {
		entries, err := fs.ReadDir("/")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			t.Error("expected entries")
		}

		names := make(map[string]bool)
		for _, e := range entries {
			names[e.Name()] = true
		}
		for _, want := range []string{"datasets", "logs", "metrics", "README.txt"} {
			if !names[want] {
				t.Errorf("missing %q", want)
			}
		}
	})

	t.Run("subdirectory", func(t *testing.T) {
		entries, err := fs.ReadDir("/logs")
		if err != nil {
			t.Fatal(err)
		}

		names := make(map[string]bool)
		for _, e := range entries {
			names[e.Name()] = true
		}
		for _, want := range []string{"schema.json", "q", "presets"} {
			if !names[want] {
				t.Errorf("missing %q", want)
			}
		}
	})

	t.Run("file fails", func(t *testing.T) {
		_, err := fs.ReadDir("/README.txt")
		if err != syscall.ENOTDIR {
			t.Errorf("expected ENOTDIR, got %v", err)
		}
	})
}

func TestMutations(t *testing.T) {
	fs := newTestFS(t)

	t.Run("Remove outside _queries fails", func(t *testing.T) {
		err := fs.Remove("/logs/schema.json")
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})

	t.Run("Rename outside _queries fails", func(t *testing.T) {
		err := fs.Rename("/logs", "/logs2")
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})

	t.Run("MkdirAll in _queries succeeds", func(t *testing.T) {
		err := fs.MkdirAll("/_queries/newdir", 0755)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("MkdirAll outside _queries fails", func(t *testing.T) {
		err := fs.MkdirAll("/newdir", 0755)
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})

	t.Run("Symlink fails", func(t *testing.T) {
		err := fs.Symlink("/logs", "/link")
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})

	t.Run("TempFile fails", func(t *testing.T) {
		_, err := fs.TempFile("/tmp", "test")
		if err != billy.ErrNotSupported {
			t.Errorf("expected ErrNotSupported, got %v", err)
		}
	})
}

func TestLstat(t *testing.T) {
	fs := newTestFS(t)

	// Lstat should behave same as Stat (no symlinks)
	info1, err1 := fs.Stat("/logs")
	info2, err2 := fs.Lstat("/logs")

	if err1 != nil || err2 != nil {
		t.Fatalf("errors: %v, %v", err1, err2)
	}
	if info1.Name() != info2.Name() {
		t.Errorf("names differ: %q vs %q", info1.Name(), info2.Name())
	}
	if info1.IsDir() != info2.IsDir() {
		t.Error("IsDir differs")
	}
}

func TestReadlink(t *testing.T) {
	fs := newTestFS(t)
	_, err := fs.Readlink("/anything")
	if err != syscall.ENOENT {
		t.Errorf("expected ENOENT, got %v", err)
	}
}

func TestJoin(t *testing.T) {
	fs := newTestFS(t)

	cases := []struct {
		parts []string
		want  string
	}{
		{[]string{"a", "b"}, "a/b"},
		{[]string{"/a", "b", "c"}, "/a/b/c"},
		{[]string{"a", "../b"}, "b"},
		{[]string{"/", "logs"}, "/logs"},
	}

	for _, tc := range cases {
		got := fs.Join(tc.parts...)
		if got != tc.want {
			t.Errorf("Join(%v) = %q, want %q", tc.parts, got, tc.want)
		}
	}
}

func TestRoot(t *testing.T) {
	fs := newTestFS(t).(*FS)
	if fs.Root() != "/" {
		t.Errorf("Root() = %q, want /", fs.Root())
	}
}

func TestChroot(t *testing.T) {
	fs := newTestFS(t)

	chrooted, err := fs.Chroot("/logs")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("root becomes /logs", func(t *testing.T) {
		if chrooted.Root() != "/logs" {
			t.Errorf("Root() = %q", chrooted.Root())
		}
	})

	t.Run("stat relative to chroot", func(t *testing.T) {
		info, err := chrooted.Stat("/schema.json")
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() != "schema.json" {
			t.Errorf("Name() = %q", info.Name())
		}
	})

	t.Run("readdir at root", func(t *testing.T) {
		entries, err := chrooted.ReadDir("/")
		if err != nil {
			t.Fatal(err)
		}
		names := make(map[string]bool)
		for _, e := range entries {
			names[e.Name()] = true
		}
		if !names["schema.json"] {
			t.Error("missing schema.json")
		}
	})

	t.Run("chroot on file fails", func(t *testing.T) {
		_, err := fs.Chroot("/README.txt")
		if err != syscall.ENOTDIR {
			t.Errorf("expected ENOTDIR, got %v", err)
		}
	})
}

func TestCapabilities(t *testing.T) {
	fs := newTestFS(t).(*FS)
	caps := fs.Capabilities()

	if caps&billy.ReadCapability == 0 {
		t.Error("missing ReadCapability")
	}
	if caps&billy.SeekCapability == 0 {
		t.Error("missing SeekCapability")
	}
	if caps&billy.WriteCapability == 0 {
		t.Error("missing WriteCapability")
	}
}

func TestChangeInterface(t *testing.T) {
	fs := newTestFS(t).(*FS)

	// These should all succeed (no-op for virtual fs)
	if err := fs.Chmod("/logs", 0755); err != nil {
		t.Errorf("Chmod: %v", err)
	}
	if err := fs.Chown("/logs", 1000, 1000); err != nil {
		t.Errorf("Chown: %v", err)
	}
	if err := fs.Lchown("/logs", 1000, 1000); err != nil {
		t.Errorf("Lchown: %v", err)
	}
}

func TestInterfaceCompliance(t *testing.T) {
	fs := newTestFS(t)

	// Verify interface compliance
	var _ billy.Filesystem = fs
	var _ billy.Change = fs.(*FS)
	var _ billy.Capable = fs.(*FS)
}

func TestChrootedFS(t *testing.T) {
	fs := newTestFS(t)
	chrooted, _ := fs.Chroot("/logs")

	t.Run("Open", func(t *testing.T) {
		f, err := chrooted.Open("/schema.json")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	})

	t.Run("ReadDir", func(t *testing.T) {
		entries, err := chrooted.ReadDir("/")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			t.Error("expected entries")
		}
	})

	t.Run("nested chroot", func(t *testing.T) {
		nested, err := chrooted.Chroot("/q")
		if err != nil {
			t.Fatal(err)
		}
		if nested.Root() != "/logs/q" {
			t.Errorf("Root() = %q", nested.Root())
		}
	})

	t.Run("mutations blocked", func(t *testing.T) {
		if err := chrooted.Remove("/schema.json"); err != syscall.EROFS {
			t.Error("expected EROFS")
		}
		if err := chrooted.Rename("/a", "/b"); err != syscall.EROFS {
			t.Error("expected EROFS")
		}
		if err := chrooted.Symlink("/a", "/b"); err != syscall.EROFS {
			t.Error("expected EROFS")
		}
		if err := chrooted.MkdirAll("/newdir", 0755); err != syscall.EROFS {
			t.Error("expected EROFS")
		}
	})

	t.Run("TempFile not supported", func(t *testing.T) {
		_, err := chrooted.TempFile("/", "test")
		if err != billy.ErrNotSupported {
			t.Error("expected ErrNotSupported")
		}
	})

	t.Run("Readlink not supported", func(t *testing.T) {
		_, err := chrooted.Readlink("/anything")
		if err != syscall.ENOENT {
			t.Error("expected ENOENT")
		}
	})

	t.Run("Change interface", func(t *testing.T) {
		c := chrooted.(*chrootFS)
		if err := c.Chmod("/", 0755); err != nil {
			t.Error(err)
		}
		if err := c.Chown("/", 0, 0); err != nil {
			t.Error(err)
		}
		if err := c.Lchown("/", 0, 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("Capabilities", func(t *testing.T) {
		caps := chrooted.(*chrootFS).Capabilities()
		if caps&billy.ReadCapability == 0 {
			t.Error("missing read")
		}
	})
}

func TestRemoveInQueries(t *testing.T) {
	fs := newTestFS(t)

	// Remove in _queries still returns EROFS (not fully implemented)
	err := fs.Remove("/_queries/test")
	if err != syscall.EROFS {
		t.Errorf("expected EROFS, got %v", err)
	}
}

func TestRenameInQueries(t *testing.T) {
	fs := newTestFS(t)

	err := fs.Rename("/_queries/a", "/_queries/b")
	if err != syscall.EROFS {
		t.Errorf("expected EROFS, got %v", err)
	}
}

func TestChrootOpenFile(t *testing.T) {
	fs := newTestFS(t)
	chrooted, _ := fs.Chroot("/logs")

	t.Run("read only", func(t *testing.T) {
		f, err := chrooted.OpenFile("/schema.json", os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	})

	t.Run("write blocked", func(t *testing.T) {
		_, err := chrooted.OpenFile("/schema.json", os.O_RDWR, 0)
		if err != syscall.EROFS {
			t.Errorf("expected EROFS, got %v", err)
		}
	})
}

func TestChrootCreate(t *testing.T) {
	fs := newTestFS(t)
	chrooted, _ := fs.Chroot("/logs")

	_, err := chrooted.Create("/newfile")
	if err != syscall.EROFS {
		t.Errorf("expected EROFS, got %v", err)
	}
}

func TestChrootStat(t *testing.T) {
	fs := newTestFS(t)
	chrooted, _ := fs.Chroot("/logs")

	info, err := chrooted.Stat("/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name() != "schema.json" {
		t.Errorf("Name = %q", info.Name())
	}

	info2, err := chrooted.Lstat("/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name() != info2.Name() {
		t.Error("Stat and Lstat differ")
	}
}

func TestChrootJoin(t *testing.T) {
	fs := newTestFS(t)
	chrooted, _ := fs.Chroot("/logs")

	got := chrooted.Join("a", "b", "c")
	if got != "a/b/c" {
		t.Errorf("Join = %q", got)
	}
}

func TestQueriesWriteFlow(t *testing.T) {
	fs := newTestFS(t)

	// Full write flow
	f, err := fs.Create("/_queries/writetest/apl")
	if err != nil {
		t.Fatal(err)
	}
	n, err := f.Write([]byte("['test'] | take 5"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 17 {
		t.Errorf("wrote %d bytes", n)
	}
	f.Close()

	// Verify can read back
	f2, err := fs.Open("/_queries/writetest/apl")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "['test'] | take 5" {
		t.Errorf("got %q", data)
	}
}

func TestFileSeekAndReadAt(t *testing.T) {
	fs := newTestFS(t)
	f, err := fs.Open("/README.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Seek to start
	pos, err := f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("pos = %d", pos)
	}

	// Read some
	buf := make([]byte, 5)
	n, _ := f.Read(buf)
	if n == 0 {
		t.Error("no data read")
	}

	// ReadAt at offset
	buf2 := make([]byte, 5)
	n2, _ := f.ReadAt(buf2, 0)
	if n2 == 0 {
		t.Error("no data from ReadAt")
	}
}
