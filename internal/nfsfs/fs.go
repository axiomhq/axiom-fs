package nfsfs

import (
	"context"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/vfs"
)

type FS struct {
	root      *vfs.Root
	rootPath  string
	sizeCache sync.Map // map[string]int64 - caches actual file sizes after Open
}

type sizedFileInfo struct {
	os.FileInfo
	size int64
}

func (s *sizedFileInfo) Size() int64 { return s.size }

func (f *FS) cacheFileSize(filename string, size int64) {
	f.sizeCache.Store(path.Clean(filename), size)
}

func (f *FS) getCachedSize(filename string) (int64, bool) {
	if v, ok := f.sizeCache.Load(path.Clean(filename)); ok {
		return v.(int64), true
	}
	return 0, false
}

func New(root *vfs.Root) *FS {
	return &FS{
		root:     root,
		rootPath: "/",
	}
}

func (f *FS) resolve(filename string) (vfs.Node, error) {
	filename = path.Clean(filename)
	if !path.IsAbs(filename) {
		filename = path.Join(f.rootPath, filename)
	}
	filename = path.Clean(filename)

	if filename == "/" || filename == "." {
		return f.root, nil
	}

	filename = strings.TrimPrefix(filename, "/")
	segments := strings.Split(filename, "/")

	ctx := context.Background()
	var current vfs.Node = f.root
	for _, seg := range segments {
		if seg == "" || seg == "." {
			continue
		}
		dir, ok := current.(vfs.Dir)
		if !ok {
			return nil, syscall.ENOTDIR
		}
		next, err := dir.Lookup(ctx, seg)
		if err != nil {
			return nil, err
		}
		current = next
	}
	return current, nil
}

func (f *FS) isQueriesPath(filename string) bool {
	filename = path.Clean(filename)
	filename = strings.TrimPrefix(filename, "/")
	return strings.HasPrefix(filename, "_queries/")
}

func (f *FS) Create(filename string) (billy.File, error) {
	if !f.isQueriesPath(filename) {
		return nil, syscall.EROFS
	}
	return f.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (f *FS) Open(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDONLY, 0)
}

func (f *FS) OpenFile(filename string, flag int, perm fs.FileMode) (billy.File, error) {
	node, err := f.resolve(filename)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	isWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0
	if isWrite {
		if !f.isQueriesPath(filename) {
			return nil, syscall.EROFS
		}
		wf, ok := node.(vfs.Writable)
		if !ok {
			return nil, syscall.EROFS
		}
		return wf.Create(ctx)
	}

	file, ok := node.(vfs.File)
	if !ok {
		return nil, syscall.EISDIR
	}
	opened, err := file.Open(ctx, flag)
	if err != nil {
		return nil, err
	}
	// Cache the opened file with its path so Stat can return accurate size
	if sizer, ok := opened.(interface{ Size() int64 }); ok {
		f.cacheFileSize(filename, sizer.Size())
	}
	return opened, nil
}

func (f *FS) Stat(filename string) (os.FileInfo, error) {
	node, err := f.resolve(filename)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	info, err := node.Stat(ctx)
	if err != nil {
		return nil, err
	}
	// Check if we have a cached actual size from a previous Open
	if cachedSize, ok := f.getCachedSize(filename); ok {
		return &sizedFileInfo{FileInfo: info, size: cachedSize}, nil
	}
	return info, nil
}

func (f *FS) Rename(oldpath, newpath string) error {
	if !f.isQueriesPath(oldpath) || !f.isQueriesPath(newpath) {
		return syscall.EROFS
	}
	return syscall.EROFS
}

func (f *FS) Remove(filename string) error {
	if !f.isQueriesPath(filename) {
		return syscall.EROFS
	}
	return syscall.EROFS
}

func (f *FS) Join(elem ...string) string {
	return path.Join(elem...)
}

func (f *FS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrNotSupported
}

func (f *FS) ReadDir(dirname string) ([]os.FileInfo, error) {
	node, err := f.resolve(dirname)
	if err != nil {
		return nil, err
	}
	dir, ok := node.(vfs.Dir)
	if !ok {
		return nil, syscall.ENOTDIR
	}
	ctx := context.Background()
	return dir.ReadDir(ctx)
}

func (f *FS) MkdirAll(filename string, perm os.FileMode) error {
	if !f.isQueriesPath(filename) {
		return syscall.EROFS
	}
	return nil
}

func (f *FS) Lstat(filename string) (os.FileInfo, error) {
	return f.Stat(filename)
}

func (f *FS) Symlink(target, link string) error {
	return syscall.EROFS
}

func (f *FS) Readlink(link string) (string, error) {
	return "", syscall.ENOENT
}

func (f *FS) Chroot(p string) (billy.Filesystem, error) {
	node, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	_, ok := node.(vfs.Dir)
	if !ok {
		return nil, syscall.ENOTDIR
	}
	newPath := path.Clean(path.Join(f.rootPath, p))
	return &chrootFS{
		parent:   f,
		rootPath: newPath,
	}, nil
}

func (f *FS) Root() string {
	return f.rootPath
}

func (f *FS) Chmod(name string, mode os.FileMode) error {
	return nil
}

func (f *FS) Lchown(name string, uid, gid int) error {
	return nil
}

func (f *FS) Chown(name string, uid, gid int) error {
	return nil
}

func (f *FS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}

func (f *FS) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.WriteCapability | billy.SeekCapability
}

type chrootFS struct {
	parent   *FS
	rootPath string
}

func (c *chrootFS) resolve(filename string) (vfs.Node, error) {
	filename = path.Clean(filename)
	if !path.IsAbs(filename) {
		filename = "/" + filename
	}
	fullPath := path.Join(c.rootPath, filename)
	return c.parent.resolve(fullPath)
}

func (c *chrootFS) isQueriesPath(filename string) bool {
	filename = path.Clean(filename)
	if !path.IsAbs(filename) {
		filename = "/" + filename
	}
	fullPath := path.Join(c.rootPath, filename)
	return c.parent.isQueriesPath(fullPath)
}

func (c *chrootFS) Create(filename string) (billy.File, error) {
	if !c.isQueriesPath(filename) {
		return nil, syscall.EROFS
	}
	return c.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (c *chrootFS) Open(filename string) (billy.File, error) {
	return c.OpenFile(filename, os.O_RDONLY, 0)
}

func (c *chrootFS) OpenFile(filename string, flag int, perm fs.FileMode) (billy.File, error) {
	node, err := c.resolve(filename)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	isWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0
	if isWrite {
		if !c.isQueriesPath(filename) {
			return nil, syscall.EROFS
		}
		wf, ok := node.(vfs.Writable)
		if !ok {
			return nil, syscall.EROFS
		}
		return wf.Create(ctx)
	}

	file, ok := node.(vfs.File)
	if !ok {
		return nil, syscall.EISDIR
	}
	return file.Open(ctx, flag)
}

func (c *chrootFS) Stat(filename string) (os.FileInfo, error) {
	node, err := c.resolve(filename)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	return node.Stat(ctx)
}

func (c *chrootFS) Rename(oldpath, newpath string) error {
	return syscall.EROFS
}

func (c *chrootFS) Remove(filename string) error {
	return syscall.EROFS
}

func (c *chrootFS) Join(elem ...string) string {
	return path.Join(elem...)
}

func (c *chrootFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrNotSupported
}

func (c *chrootFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	node, err := c.resolve(dirname)
	if err != nil {
		return nil, err
	}
	dir, ok := node.(vfs.Dir)
	if !ok {
		return nil, syscall.ENOTDIR
	}
	ctx := context.Background()
	return dir.ReadDir(ctx)
}

func (c *chrootFS) MkdirAll(filename string, perm os.FileMode) error {
	if !c.isQueriesPath(filename) {
		return syscall.EROFS
	}
	return nil
}

func (c *chrootFS) Lstat(filename string) (os.FileInfo, error) {
	return c.Stat(filename)
}

func (c *chrootFS) Symlink(target, link string) error {
	return syscall.EROFS
}

func (c *chrootFS) Readlink(link string) (string, error) {
	return "", syscall.ENOENT
}

func (c *chrootFS) Chroot(p string) (billy.Filesystem, error) {
	newPath := path.Clean(path.Join(c.rootPath, p))
	return &chrootFS{
		parent:   c.parent,
		rootPath: newPath,
	}, nil
}

func (c *chrootFS) Root() string {
	return c.rootPath
}

func (c *chrootFS) Chmod(name string, mode os.FileMode) error {
	return nil
}

func (c *chrootFS) Lchown(name string, uid, gid int) error {
	return nil
}

func (c *chrootFS) Chown(name string, uid, gid int) error {
	return nil
}

func (c *chrootFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}

func (c *chrootFS) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.WriteCapability | billy.SeekCapability
}

var _ billy.Filesystem = (*FS)(nil)
var _ billy.Change = (*FS)(nil)
var _ billy.Capable = (*FS)(nil)

var _ billy.Filesystem = (*chrootFS)(nil)
var _ billy.Change = (*chrootFS)(nil)
var _ billy.Capable = (*chrootFS)(nil)
