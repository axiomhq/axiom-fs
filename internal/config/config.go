package config

import (
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	MountPoint       string
	DefaultRange     string
	DefaultLimit     int
	MaxLimit         int
	MaxRange         time.Duration
	CacheTTL         time.Duration
	MaxCacheEntries  int
	MaxCacheBytes    int
	MaxInMemoryBytes int
	CacheDir         string
	QueryDir         string
	TempDir          string
	SampleLimit      int

	AxiomURL   string
	AxiomToken string
	AxiomOrgID string
}

func Default() Config {
	queryDir := ""
	if dir, err := os.UserConfigDir(); err == nil {
		queryDir = filepath.Join(dir, "axiom-fs", "queries")
	} else if home, err := os.UserHomeDir(); err == nil {
		queryDir = filepath.Join(home, ".axiom-fs", "queries")
	} else {
		queryDir = "axiom-fs-queries"
	}
	cacheDir := ""
	if dir, err := os.UserConfigDir(); err == nil {
		cacheDir = filepath.Join(dir, "axiom-fs", "cache")
	} else if home, err := os.UserHomeDir(); err == nil {
		cacheDir = filepath.Join(home, ".axiom-fs", "cache")
	} else {
		cacheDir = "axiom-fs-cache"
	}
	return Config{
		MountPoint:       "/mnt/axiom",
		DefaultRange:     "1h",
		DefaultLimit:     10000,
		MaxLimit:         100000,
		MaxRange:         24 * time.Hour,
		CacheTTL:         60 * time.Second,
		MaxCacheEntries:  256,
		MaxCacheBytes:    50 << 20,
		MaxInMemoryBytes: 8 << 20,
		CacheDir:         cacheDir,
		QueryDir:         queryDir,
		TempDir:          "",
		SampleLimit:      100,
	}
}
