package vfs

import (
	"bytes"
	"io"
	"os"

	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/query"
	"github.com/axiomhq/axiom-fs/internal/store"
)

type bytesFile struct {
	name   string
	reader *bytes.Reader
}

func newBytesFile(data []byte) billy.File {
	return &bytesFile{reader: bytes.NewReader(data)}
}

func (f *bytesFile) Name() string { return f.name }

func (f *bytesFile) Read(p []byte) (int, error) {
	return f.reader.Read(p)
}

func (f *bytesFile) ReadAt(p []byte, off int64) (int, error) {
	return f.reader.ReadAt(p, off)
}

func (f *bytesFile) Seek(offset int64, whence int) (int64, error) {
	return f.reader.Seek(offset, whence)
}

func (f *bytesFile) Write(p []byte) (int, error) {
	return 0, os.ErrPermission
}

func (f *bytesFile) Close() error {
	return nil
}

func (f *bytesFile) Lock() error   { return nil }
func (f *bytesFile) Unlock() error { return nil }
func (f *bytesFile) Truncate(size int64) error {
	return os.ErrPermission
}

type tempFile struct {
	file *os.File
	size int64
}

func newTempFile(file *os.File, size int64) billy.File {
	return &tempFile{file: file, size: size}
}

func (f *tempFile) Name() string { return f.file.Name() }

func (f *tempFile) Read(p []byte) (int, error) {
	return f.file.Read(p)
}

func (f *tempFile) ReadAt(p []byte, off int64) (int, error) {
	return f.file.ReadAt(p, off)
}

func (f *tempFile) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f *tempFile) Write(p []byte) (int, error) {
	return 0, os.ErrPermission
}

func (f *tempFile) Close() error {
	name := f.file.Name()
	_ = f.file.Close()
	_ = os.Remove(name)
	return nil
}

func (f *tempFile) Lock() error   { return nil }
func (f *tempFile) Unlock() error { return nil }
func (f *tempFile) Truncate(size int64) error {
	return os.ErrPermission
}

type aplFile struct {
	store   *store.QueryStore
	name    string
	buf     bytes.Buffer
	written bool
}

func newAPLFile(s *store.QueryStore, name string) billy.File {
	return &aplFile{store: s, name: name}
}

func (f *aplFile) Name() string { return "apl" }

func (f *aplFile) Read(p []byte) (int, error) {
	data := f.store.Get(f.name)
	return bytes.NewReader(data).Read(p)
}

func (f *aplFile) ReadAt(p []byte, off int64) (int, error) {
	data := f.store.Get(f.name)
	return bytes.NewReader(data).ReadAt(p, off)
}

func (f *aplFile) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *aplFile) Write(p []byte) (int, error) {
	f.written = true
	return f.buf.Write(p)
}

func (f *aplFile) Close() error {
	if f.written {
		f.store.Set(f.name, f.buf.Bytes())
	}
	return nil
}

func (f *aplFile) Lock() error   { return nil }
func (f *aplFile) Unlock() error { return nil }
func (f *aplFile) Truncate(size int64) error {
	if size == 0 {
		f.store.Truncate(f.name)
		f.buf.Reset()
	}
	return nil
}

func openResult(result query.ResultData) (billy.File, error) {
	if result.File != nil {
		_, _ = result.File.Seek(0, io.SeekStart)
		return newTempFile(result.File, result.Size), nil
	}
	return newBytesFile(result.Bytes), nil
}
