package vfs

import (
	"context"
	"os"
	"time"

	"github.com/go-git/go-billy/v5"
)

type Node interface {
	Stat(ctx context.Context) (os.FileInfo, error)
}

type Dir interface {
	Node
	Lookup(ctx context.Context, name string) (Node, error)
	ReadDir(ctx context.Context) ([]os.FileInfo, error)
}

type File interface {
	Node
	Open(ctx context.Context, flags int) (billy.File, error)
}

type Writable interface {
	File
	Create(ctx context.Context) (billy.File, error)
}

type virtualFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (v *virtualFileInfo) Name() string       { return v.name }
func (v *virtualFileInfo) Size() int64        { return v.size }
func (v *virtualFileInfo) Mode() os.FileMode  { return v.mode }
func (v *virtualFileInfo) ModTime() time.Time { return v.modTime }
func (v *virtualFileInfo) IsDir() bool        { return v.isDir }
func (v *virtualFileInfo) Sys() any           { return nil }

func DirInfo(name string) os.FileInfo {
	return &virtualFileInfo{
		name:    name,
		mode:    os.ModeDir | 0o555,
		modTime: time.Now(),
		isDir:   true,
	}
}

func FileInfo(name string, size int64) os.FileInfo {
	return &virtualFileInfo{
		name:    name,
		size:    size,
		mode:    0o444,
		modTime: time.Now(),
	}
}

func WritableFileInfo(name string, size int64) os.FileInfo {
	return &virtualFileInfo{
		name:    name,
		size:    size,
		mode:    0o644,
		modTime: time.Now(),
	}
}

type StaticFile struct {
	name string
	data []byte
}

func (s *StaticFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo(s.name, int64(len(s.data))), nil
}

func (s *StaticFile) Open(ctx context.Context, flags int) (billy.File, error) {
	return newBytesFile(s.data), nil
}

type ExamplesDir struct{}

func (e *ExamplesDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("examples"), nil
}

func (e *ExamplesDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	return []os.FileInfo{FileInfo("quickstart.txt", 0)}, nil
}

func (e *ExamplesDir) Lookup(ctx context.Context, name string) (Node, error) {
	if name == "quickstart.txt" {
		return &StaticFile{name: name, data: exampleText}, nil
	}
	return nil, os.ErrNotExist
}
