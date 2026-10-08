package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type owner struct {
	parent, lock int
	name         string
}

func acquire(path string, create bool) (*owner, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00?#") {
		return nil, fail("invalid_argument", "canonical absolute deployment database grant required")
	}
	fd, e := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Dir(path), "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		unix.Close(fd)
		if err != nil {
			return nil, err
		}
		fd = next
	}
	cleanup := func(err error) (*owner, error) { unix.Close(fd); return nil, err }
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0700 {
		return cleanup(fail("permission_denied", "database directory must be owned mode0700"))
	}
	name := filepath.Base(path)
	lock, e := unix.Openat(fd, name+".owner.lock", unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return cleanup(e)
	}
	if e = unix.Fstat(lock, &st); e != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0600 {
		unix.Close(lock)
		return cleanup(fail("permission_denied", "unsafe owner lock"))
	}
	if e = unix.Flock(lock, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		unix.Close(lock)
		return cleanup(fail("conflict", "database already owned"))
	}
	o := &owner{parent: fd, lock: lock, name: name}
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	dbfd, e := unix.Openat(fd, name, flags, 0600)
	if e != nil {
		o.close()
		return nil, e
	}
	e = unix.Fstat(dbfd, &st)
	unix.Close(dbfd)
	if e != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0600 {
		o.close()
		return nil, fail("permission_denied", "database must be private owned single-link regular file")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		err := unix.Fstatat(fd, name+suffix, &st, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, unix.ENOENT) {
			continue
		}
		if err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 {
			o.close()
			return nil, fail("permission_denied", "unsafe SQLite sidecar")
		}
	}
	return o, nil
}
func (o *owner) path() string { return fmt.Sprintf("/proc/self/fd/%d/%s", o.parent, o.name) }
func (o *owner) close() {
	if o == nil {
		return
	}
	unix.Flock(o.lock, unix.LOCK_UN)
	unix.Close(o.lock)
	unix.Close(o.parent)
}
