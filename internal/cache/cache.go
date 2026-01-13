package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Entry struct {
	Bytes     []byte
	ExpiresAt time.Time
}

type Cache struct {
	mu         sync.Mutex
	items      map[string]Entry
	ttl        time.Duration
	order      []string
	size       int
	maxEntries int
	maxBytes   int
	dir        string
}

func New(ttl time.Duration, maxEntries, maxBytes int, dir string) *Cache {
	if dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	return &Cache{
		items:      make(map[string]Entry),
		ttl:        ttl,
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		dir:        dir,
	}
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		if c.dir != "" {
			return c.getDiskLocked(key)
		}
		return nil, false
	}
	if c.ttl > 0 && time.Now().After(entry.ExpiresAt) {
		c.removeLocked(key)
		if c.dir != "" {
			return c.getDiskLocked(key)
		}
		return nil, false
	}
	return entry.Bytes, true
}

func (c *Cache) Set(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		c.size -= len(entry.Bytes)
		c.removeKeyLocked(key)
	}

	entry := Entry{
		Bytes:     value,
		ExpiresAt: time.Now().Add(c.ttl),
	}
	c.items[key] = entry
	c.order = append(c.order, key)
	c.size += len(value)
	c.evictLocked()

	if c.dir != "" && c.shouldPersist(len(value)) {
		_ = c.writeDiskLocked(key, value)
		c.evictDiskLocked()
	}
}

func (c *Cache) removeLocked(key string) {
	if entry, ok := c.items[key]; ok {
		c.size -= len(entry.Bytes)
		delete(c.items, key)
		c.removeKeyLocked(key)
	}
}

func (c *Cache) removeKeyLocked(key string) {
	for i, existing := range c.order {
		if existing == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

func (c *Cache) evictLocked() {
	for c.shouldEvictLocked() {
		if len(c.order) == 0 {
			return
		}
		key := c.order[0]
		c.order = c.order[1:]
		if entry, ok := c.items[key]; ok {
			c.size -= len(entry.Bytes)
			delete(c.items, key)
		}
	}
}

func (c *Cache) shouldEvictLocked() bool {
	if c.maxEntries > 0 && len(c.items) > c.maxEntries {
		return true
	}
	if c.maxBytes > 0 && c.size > c.maxBytes {
		return true
	}
	return false
}

func (c *Cache) shouldPersist(size int) bool {
	if c.maxBytes > 0 && size > c.maxBytes {
		return false
	}
	return true
}

func (c *Cache) getDiskLocked(key string) ([]byte, bool) {
	path := c.diskPath(key)
	info, err := os.Stat(path)
	if err != nil {
		return nil, false
	}
	if c.ttl > 0 && time.Since(info.ModTime()) > c.ttl {
		_ = os.Remove(path)
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	_ = os.Chtimes(path, time.Now(), time.Now())
	c.items[key] = Entry{Bytes: data, ExpiresAt: time.Now().Add(c.ttl)}
	c.order = append(c.order, key)
	c.size += len(data)
	c.evictLocked()
	return data, true
}

func (c *Cache) writeDiskLocked(key string, data []byte) error {
	path := c.diskPath(key)
	tmp, err := os.CreateTemp(c.dir, "cache-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func (c *Cache) diskPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:]))
}

func (c *Cache) evictDiskLocked() {
	if c.dir == "" {
		return
	}
	entries, total := c.listDiskLocked()
	for c.shouldEvictDisk(total, len(entries)) {
		if len(entries) == 0 {
			return
		}
		entry := entries[0]
		_ = os.Remove(entry.path)
		total -= entry.size
		entries = entries[1:]
	}
}

func (c *Cache) listDiskLocked() ([]diskEntry, int) {
	entries := []diskEntry{}
	total := 0
	items, err := os.ReadDir(c.dir)
	if err != nil {
		return entries, total
	}
	for _, item := range items {
		info, err := item.Info()
		if err != nil {
			continue
		}
		if c.ttl > 0 && time.Since(info.ModTime()) > c.ttl {
			_ = os.Remove(filepath.Join(c.dir, item.Name()))
			continue
		}
		entries = append(entries, diskEntry{
			path: filepath.Join(c.dir, item.Name()),
			mod:  info.ModTime(),
			size: int(info.Size()),
		})
		total += int(info.Size())
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].mod.Before(entries[j].mod) })
	return entries, total
}

func (c *Cache) shouldEvictDisk(total int, count int) bool {
	if c.maxEntries > 0 && count > c.maxEntries {
		return true
	}
	if c.maxBytes > 0 && total > c.maxBytes {
		return true
	}
	return false
}

type diskEntry struct {
	path string
	mod  time.Time
	size int
}
