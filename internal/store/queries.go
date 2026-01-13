package store

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type QueryStore struct {
	mu  sync.Mutex
	dir string
}

func NewQueryStore(dir string) *QueryStore {
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "axiom-fs-queries")
	}
	_ = os.MkdirAll(dir, 0o755)
	return &QueryStore{dir: dir}
}

func (s *QueryStore) Get(name string) []byte {
	if !isValidName(name) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, _ := os.ReadFile(filepath.Join(s.dir, name+".apl"))
	return data
}

func (s *QueryStore) Set(name string, data []byte) {
	if !isValidName(name) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, name+".apl")
	tmp, err := os.CreateTemp(s.dir, "apl-*")
	if err != nil {
		return
	}
	_, _ = tmp.Write(data)
	_ = tmp.Close()
	_ = os.Rename(tmp.Name(), path)
}

func (s *QueryStore) Truncate(name string) {
	if !isValidName(name) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, name+".apl")
	_ = os.WriteFile(path, nil, 0o644)
}

func (s *QueryStore) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".apl")
		if isValidName(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func isValidName(name string) bool {
	if name == "" {
		return false
	}
	if len(name) > 64 {
		return false
	}
	if strings.Contains(name, "/") || strings.Contains(name, string(os.PathSeparator)) {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	for _, r := range name {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		switch r {
		case '-', '_', '.':
			continue
		default:
			return false
		}
	}
	return true
}
