package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCacheBasicGetSet(t *testing.T) {
	c := New(time.Hour, 100, 0, "")

	t.Run("get missing key", func(t *testing.T) {
		_, ok := c.Get("missing")
		if ok {
			t.Error("expected missing key to return false")
		}
	})

	t.Run("set and get", func(t *testing.T) {
		c.Set("key1", []byte("value1"))
		got, ok := c.Get("key1")
		if !ok {
			t.Fatal("expected key to exist")
		}
		if string(got) != "value1" {
			t.Errorf("got %q, want %q", got, "value1")
		}
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		c.Set("key1", []byte("updated"))
		got, ok := c.Get("key1")
		if !ok {
			t.Fatal("expected key to exist")
		}
		if string(got) != "updated" {
			t.Errorf("got %q, want %q", got, "updated")
		}
	})
}

func TestCacheTTLExpiration(t *testing.T) {
	c := New(50*time.Millisecond, 100, 0, "")

	c.Set("expires", []byte("data"))

	got, ok := c.Get("expires")
	if !ok {
		t.Fatal("key should exist before expiration")
	}
	if string(got) != "data" {
		t.Errorf("got %q, want %q", got, "data")
	}

	time.Sleep(100 * time.Millisecond)

	_, ok = c.Get("expires")
	if ok {
		t.Error("key should have expired")
	}
}

func TestCacheMaxEntriesEviction(t *testing.T) {
	c := New(time.Hour, 3, 0, "")

	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3"))

	if _, ok := c.Get("a"); !ok {
		t.Error("key a should exist")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("key b should exist")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("key c should exist")
	}

	c.Set("d", []byte("4"))

	if _, ok := c.Get("a"); ok {
		t.Error("key a should have been evicted (oldest)")
	}
	if _, ok := c.Get("d"); !ok {
		t.Error("key d should exist")
	}
}

func TestCacheMaxBytesEviction(t *testing.T) {
	c := New(time.Hour, 0, 10, "")

	c.Set("a", []byte("123"))
	c.Set("b", []byte("456"))
	c.Set("c", []byte("789"))

	if _, ok := c.Get("a"); !ok {
		t.Error("key a should exist")
	}

	c.Set("d", []byte("0000"))

	if _, ok := c.Get("a"); ok {
		t.Error("key a should have been evicted (exceeds max bytes)")
	}
	if _, ok := c.Get("d"); !ok {
		t.Error("key d should exist")
	}
}

func TestCacheDiskPersistence(t *testing.T) {
	dir := t.TempDir()
	c := New(time.Hour, 100, 0, dir)

	c.Set("disk-key", []byte("disk-value"))

	got, ok := c.Get("disk-key")
	if !ok {
		t.Fatal("key should exist")
	}
	if string(got) != "disk-value" {
		t.Errorf("got %q, want %q", got, "disk-value")
	}

	c2 := New(time.Hour, 100, 0, dir)

	got2, ok := c2.Get("disk-key")
	if !ok {
		t.Fatal("key should exist in new cache instance")
	}
	if string(got2) != "disk-value" {
		t.Errorf("got %q, want %q", got2, "disk-value")
	}
}

func TestCacheDiskTTLExpiration(t *testing.T) {
	dir := t.TempDir()
	c := New(50*time.Millisecond, 100, 0, dir)

	c.Set("disk-expires", []byte("data"))

	c2 := New(50*time.Millisecond, 100, 0, dir)
	got, ok := c2.Get("disk-expires")
	if !ok {
		t.Fatal("key should exist before expiration")
	}
	if string(got) != "data" {
		t.Errorf("got %q, want %q", got, "data")
	}

	time.Sleep(100 * time.Millisecond)

	c3 := New(50*time.Millisecond, 100, 0, dir)
	_, ok = c3.Get("disk-expires")
	if ok {
		t.Error("key should have expired on disk")
	}
}

func TestCacheDiskEviction(t *testing.T) {
	dir := t.TempDir()
	c := New(time.Hour, 2, 0, dir)

	c.Set("x", []byte("1"))
	c.Set("y", []byte("2"))
	c.Set("z", []byte("3"))

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) > 2 {
		t.Errorf("expected at most 2 disk entries, got %d", len(entries))
	}
}

func TestCacheNoDir(t *testing.T) {
	c := New(time.Hour, 100, 0, "")

	c.Set("key", []byte("value"))
	got, ok := c.Get("key")
	if !ok {
		t.Fatal("key should exist")
	}
	if string(got) != "value" {
		t.Errorf("got %q", got)
	}
}

func TestCacheShouldPersist(t *testing.T) {
	c := New(time.Hour, 0, 10, "")

	if !c.shouldPersist(5) {
		t.Error("should persist small values")
	}
	if c.shouldPersist(20) {
		t.Error("should not persist values larger than maxBytes")
	}
}

func TestCacheDiskPath(t *testing.T) {
	c := New(time.Hour, 0, 0, "/tmp/cache")

	path := c.diskPath("testkey")
	if !filepath.IsAbs(path) {
		t.Error("path should be absolute")
	}
	if filepath.Dir(path) != "/tmp/cache" {
		t.Errorf("path dir = %q, want /tmp/cache", filepath.Dir(path))
	}
}

func TestCacheZeroTTL(t *testing.T) {
	c := New(0, 100, 0, "")

	c.Set("key", []byte("value"))
	got, ok := c.Get("key")
	if !ok {
		t.Fatal("key should exist with zero TTL (no expiration)")
	}
	if string(got) != "value" {
		t.Errorf("got %q", got)
	}
}

func TestCacheConcurrency(t *testing.T) {
	c := New(time.Hour, 1000, 0, "")

	done := make(chan bool)
	for i := range 10 {
		go func(id int) {
			for j := range 100 {
				key := string(rune('a'+id)) + string(rune('0'+j%10))
				c.Set(key, []byte("data"))
				c.Get(key)
			}
			done <- true
		}(i)
	}

	for range 10 {
		<-done
	}
}
