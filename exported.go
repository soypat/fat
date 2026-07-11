package fat

import (
	"errors"
	"io"
	"io/fs"
	"math"
	"time"
	"unsafe"
)

// Mode represents the file access mode used in Open.
type Mode uint8

// File access modes for calling Open.
const (
	ModeRead  Mode = Mode(faRead)
	ModeWrite Mode = Mode(faWrite)
	ModeRW    Mode = ModeRead | ModeWrite

	ModeCreateNew    Mode = Mode(faCreateNew)
	ModeCreateAlways Mode = Mode(faCreateAlways)
	ModeOpenExisting Mode = Mode(faOpenExisting)
	ModeOpenAppend   Mode = Mode(faOpenAppend)
	// ModeOpenAlways opens the file, creating it if it does not exist. Unlike
	// ModeCreateAlways the contents of an existing file are preserved and the
	// read/write pointer starts at the beginning of the file.
	ModeOpenAlways Mode = Mode(faOpenAlways)

	allowedModes = ModeRead | ModeWrite | ModeCreateNew | ModeCreateAlways | ModeOpenExisting | ModeOpenAppend | ModeOpenAlways
)

var (
	errInvalidMode    = errors.New("invalid fat access mode")
	errForbiddenMode  = errors.New("forbidden fat access mode")
	errWhence         = errors.New("fat: invalid whence")
	errNegativeSeek   = errors.New("fat: negative seek position")
	errNegativeOffset = errors.New("fat: negative offset")
)

// Dir represents an open FAT directory.
type Dir struct {
	dir
	inlineInfo FileInfo
}

// Mount mounts the FAT file system on the given block device and sector size.
// It immediately invalidates previously open files and directories pointing to the same FS.
// Mode should be ModeRead, ModeWrite, or both.
func (fsys *FS) Mount(bd BlockDevice, blockSize int, mode Mode) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if mode&^(ModeRead|ModeWrite) != 0 {
		return errInvalidMode
	} else if blockSize > math.MaxUint16 {
		return errors.New("sector size too large")
	}
	fr := fsys.mount_volume(bd, uint16(blockSize), uint8(mode))
	if fr != frOK {
		return fr
	}
	return nil
}

// OpenFile opens the named file for reading or writing, depending on the mode.
// The path must be absolute (starting with a slash) and must not contain
// any elements that are "." or "..".
func (fsys *FS) OpenFile(fp *File, path string, mode Mode) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	prohibited := (mode & ModeRW) &^ fsys.perm
	if mode&^allowedModes != 0 {
		return errInvalidMode
	} else if prohibited != 0 {
		return errForbiddenMode
	}
	fr := fsys.f_open(fp, path, uint8(mode))
	if fr != frOK {
		return fr
	}
	return nil
}

// lock acquires the file's filesystem lock, guarding against a concurrent
// Close: the handle is validated once the lock is held since Close
// invalidates it (by id, never by clearing obj.fs) under the same lock,
// making the unsynchronized read of obj.fs safe. On success the FS is
// returned locked and the caller must Unlock it.
func (fp *File) lock() (*FS, fileResult) {
	fsys := fp.obj.fs
	if fsys == nil {
		return nil, frInvalidObject
	}
	fsys.mu.Lock()
	if fr := fp.obj.validate(); fr != frOK {
		fsys.mu.Unlock()
		return nil, fr
	}
	return fsys, frOK
}

// lock is the Dir counterpart of (*File).lock.
func (dp *Dir) lock() (*FS, fileResult) {
	fsys := dp.obj.fs
	if fsys == nil {
		return nil, frInvalidObject
	}
	fsys.mu.Lock()
	if fr := dp.obj.validate(); fr != frOK {
		fsys.mu.Unlock()
		return nil, fr
	}
	return fsys, frOK
}

// Read reads up to len(buf) bytes from the File. It implements the [io.Reader] interface.
func (fp *File) Read(buf []byte) (int, error) {
	fsys, fr := fp.lock()
	if fr != frOK {
		return 0, fr
	}
	defer fsys.mu.Unlock()
	br, fr := fp.f_read(buf)
	if fr != frOK {
		return br, fr
	} else if br == 0 && fr == frOK {
		return br, io.EOF
	}
	return br, nil
}

// Write writes len(buf) bytes to the File. It implements the [io.Writer] interface.
func (fp *File) Write(buf []byte) (int, error) {
	fsys, fr := fp.lock()
	if fr != frOK {
		return 0, fr
	}
	defer fsys.mu.Unlock()
	bw, fr := fp.f_write(buf)
	if fr != frOK {
		return bw, fr
	} else if bw < len(buf) {
		return bw, io.ErrShortWrite // Disk full.
	}
	return bw, nil
}

// WriteString writes the contents of s to the File without copying it
// to a byte slice. It implements the [io.StringWriter] interface.
func (fp *File) WriteString(s string) (int, error) {
	// f_write never modifies the source buffer so this is safe.
	return fp.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// ReadAt reads len(p) bytes from the File starting at byte offset off. It
// implements the [io.ReaderAt] interface: the offset used by Read, Write and
// Seek is saved and restored, so ReadAt neither affects nor is affected by it.
// When fewer than len(p) bytes are read it returns a non-nil error (io.EOF at
// end of file).
func (fp *File) ReadAt(p []byte, off int64) (int, error) {
	fsys, fr := fp.lock()
	if fr != frOK {
		return 0, fr
	}
	defer fsys.mu.Unlock()
	if off < 0 {
		return 0, errNegativeOffset
	} else if off >= fp.obj.objsize {
		// Checked before seeking since f_lseek past EOF in write mode
		// would grow the file.
		return 0, io.EOF
	}
	cur := fp.fptr
	fr = fp.f_lseek(off)
	if fr != frOK {
		return 0, fr
	}
	n, fr := fp.f_read(p)
	if frs := fp.f_lseek(cur); fr == frOK {
		fr = frs
	}
	if fr != frOK {
		return n, fr
	} else if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt writes len(p) bytes to the File starting at byte offset off. It
// implements the [io.WriterAt] interface: the offset used by Read, Write and
// Seek is saved and restored, so WriteAt does not affect it. Writing past the
// end of the file extends it; the contents of any gap are undefined.
func (fp *File) WriteAt(p []byte, off int64) (int, error) {
	fsys, fr := fp.lock()
	if fr != frOK {
		return 0, fr
	}
	defer fsys.mu.Unlock()
	if off < 0 {
		return 0, errNegativeOffset
	} else if fp.flag&faWrite == 0 {
		return 0, frWriteProtected
	}
	cur := fp.fptr
	fr = fp.f_lseek(off)
	if fr != frOK {
		return 0, fr
	}
	n, fr := fp.f_write(p)
	if frs := fp.f_lseek(cur); fr == frOK {
		fr = frs
	}
	if fr != frOK {
		return n, fr
	} else if n < len(p) {
		return n, io.ErrShortWrite // Disk full.
	}
	return n, nil
}

// Size returns the current size of the file in bytes, including data
// written but not yet synced to the device.
func (fp *File) Size() int64 {
	fsys := fp.obj.fs
	if fsys == nil {
		return fp.obj.objsize
	}
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return fp.obj.objsize
}

// Truncate changes the size of the file to size, discarding data past the new
// end when shrinking and extending the file when growing (the contents of the
// extension are undefined). The file must be open for writing. If the current
// offset is beyond the new size it is moved to the new end of file; otherwise
// Truncate does not affect it.
func (fp *File) Truncate(size int64) error {
	fsys, fr := fp.lock()
	if fr != frOK {
		return fr
	}
	defer fsys.mu.Unlock()
	if fp.err != frOK {
		return fp.err
	} else if size < 0 {
		return errNegativeOffset
	} else if fp.flag&faWrite == 0 || fsys.perm&ModeWrite == 0 {
		return frWriteProtected
	}
	cur := fp.fptr
	// Seeking grows the cluster chain when size exceeds the file size,
	// and positions fptr for f_truncate to shrink to when below it.
	fr = fp.f_lseek(size)
	if fr == frOK {
		fr = fp.f_truncate()
	}
	if cur > size {
		cur = size
	}
	if frs := fp.f_lseek(cur); fr == frOK {
		fr = frs
	}
	if fr != frOK {
		return fr
	}
	return nil
}

// Seek sets the offset for the next Read or Write on the file to offset,
// interpreted according to whence: [io.SeekStart], [io.SeekCurrent] or
// [io.SeekEnd]. It returns the new offset and implements the [io.Seeker]
// interface. Seeking past the end of a file open for writing extends the
// file; the contents of the gap are undefined.
func (fp *File) Seek(offset int64, whence int) (int64, error) {
	fsys, fr := fp.lock()
	if fr != frOK {
		return 0, fr
	}
	defer fsys.mu.Unlock()
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = fp.fptr + offset
	case io.SeekEnd:
		abs = fp.obj.objsize + offset
	default:
		return 0, errWhence
	}
	if abs < 0 {
		return 0, errNegativeSeek
	}
	fr = fp.f_lseek(abs)
	if fr != frOK {
		return 0, fr
	}
	return fp.fptr, nil
}

// Close closes the file and syncs any unwritten data to the underlying device.
func (fp *File) Close() error {
	fsys, fr := fp.lock()
	if fr != frOK {
		return fr
	}
	defer fsys.mu.Unlock()
	fr = fp.f_close()
	if fr != frOK {
		return fr
	}
	return nil
}

// Unmount unmounts the FAT filesystem, syncing any pending writes to the
// underlying device and invalidating all open files and directories pointing
// to it. The FS can be reused by calling Mount again.
func (fsys *FS) Unmount() error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if fsys.fstype == fstypeUnknown {
		return frNoFilesystem // Not mounted.
	}
	var fr fileResult = frOK
	if fsys.perm&ModeWrite != 0 {
		fr = fsys.sync()
	}
	fsys.fstype = fstypeUnknown // Invalidate the filesystem object.
	fsys.id++                   // Invalidate open files and directories.
	fsys.perm = 0
	fsys.device = nil
	if fr != frOK {
		return fr
	}
	return nil
}

// Mkdir creates a new directory with the given path. The parent directory
// must already exist.
func (fsys *FS) Mkdir(path string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fr := fsys.f_mkdir(path)
	if fr != frOK {
		return fr
	}
	return nil
}

// Rename renames (moves) oldpath to newpath, which may be in a different
// directory. Neither file may be open. If newpath already exists Rename fails.
func (fsys *FS) Rename(oldpath, newpath string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fr := fsys.f_rename(oldpath, newpath)
	if fr != frOK {
		return fr
	}
	return nil
}

// Stat stores information describing the named file or directory into info.
func (fsys *FS) Stat(path string, info *FileInfo) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fr := fsys.f_stat(path, info)
	if fr != frOK {
		return fr
	}
	return nil
}

// Remove removes the named file or empty directory from the filesystem.
func (fsys *FS) Remove(path string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fr := fsys.f_unlink(path)
	if fr != frOK {
		return fr
	}
	return nil
}

// Sync commits all pending writes of the filesystem to the underlying device.
func (fsys *FS) Sync() error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fr := fsys.sync()
	if fr != frOK {
		return fr
	}
	return nil
}

// Sync commits the current contents of the file to the filesystem immediately.
// It flushes the file's cached data and its directory entry to the device.
func (fp *File) Sync() error {
	fsys, fr := fp.lock()
	if fr != frOK {
		return fr
	}
	defer fsys.mu.Unlock()
	fr = fsys.f_sync(fp)
	if fr != frOK {
		return fr
	}
	return nil
}

// Mode returns the lowest 2 bits of the file's permission (read, write or both).
func (fp *File) Mode() Mode {
	if fsys := fp.obj.fs; fsys != nil {
		fsys.mu.Lock()
		defer fsys.mu.Unlock()
	}
	return Mode(fp.flag & 3)
}

// OpenDir opens the named directory for reading.
func (fsys *FS) OpenDir(dp *Dir, path string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fr := fsys.f_opendir(&dp.dir, path)
	if fr != frOK {
		return fr
	}
	return nil
}

// Close closes the directory, invalidating the handle. A directory holds no
// unwritten state, so unlike (*File).Close this flushes nothing to the device.
// The Dir can be reused by passing it to OpenDir again.
func (dp *Dir) Close() error {
	fsys, fr := dp.lock()
	if fr != frOK {
		return fr
	}
	defer fsys.mu.Unlock()
	// Invalidate the handle by id instead of clearing obj.fs, matching
	// (*File).f_close: the exported lock path reads obj.fs unsynchronized.
	dp.obj.id--
	return nil
}

// ForEachFile calls the callback function for each file in the directory.
//
// The callback runs with the filesystem lock held: calling any method of
// the same FS or of its files from within the callback deadlocks.
func (dp *Dir) ForEachFile(callback func(*FileInfo) error) error {
	fsys, fr := dp.lock()
	if fr != frOK {
		return fr
	}
	defer fsys.mu.Unlock()
	if fsys.perm&ModeRead == 0 {
		return errForbiddenMode
	}

	fr = dp.sdi(0) // Rewind directory.
	if fr != frOK {
		return fr
	}
	for {
		fr := dp.f_readdir(&dp.inlineInfo)
		if fr != frOK {
			return fr
		} else if dp.inlineInfo.fname[0] == 0 {
			return nil // End of directory.
		}
		err := callback(&dp.inlineInfo)
		if err != nil {
			return err
		}
	}
}

// Rewind resets the directory cursor to the first entry, so the next
// ReadNext starts the walk over. It is the only way to restart a walk: a
// directory cursor cannot be seeked to an arbitrary entry.
func (dp *Dir) Rewind() error {
	fsys, fr := dp.lock()
	if fr != frOK {
		return fr
	}
	defer fsys.mu.Unlock()
	fr = dp.sdi(0) // Rewind directory.
	if fr != frOK {
		return fr
	}
	return nil
}

// ReadNext reads the next directory entry into dst. The "." and ".."
// pseudo-entries and the volume label are skipped. It returns io.EOF once
// the directory is exhausted; calls past that keep returning io.EOF until
// Rewind.
//
// Unlike ForEachFile, ReadNext holds the filesystem lock only for the
// duration of the call, so the FS may be used between entries. The walk is
// then not atomic: entries added or removed mid-walk may be skipped or
// repeated.
func (dp *Dir) ReadNext(dst *FileInfo) error {
	fsys, fr := dp.lock()
	if fr != frOK {
		return fr
	}
	defer fsys.mu.Unlock()
	if fsys.perm&ModeRead == 0 {
		return errForbiddenMode
	}
	fr = dp.f_readdir(dst)
	if fr != frOK {
		return fr
	} else if dst.fname[0] == 0 {
		return io.EOF
	}
	return nil
}

var _ fs.FileInfo = (*FileInfo)(nil)

// AlternateName returns the alternate name of the file.
func (finfo *FileInfo) AlternateName() string {
	return str(finfo.altname[:])
}

// Name returns the name of the file.
func (finfo *FileInfo) Name() string {
	return string(finfo.nameview())
}

// AppendName appends the name of the file to dst.
func (finfo *FileInfo) AppendName(dst []byte) []byte {
	return append(dst, finfo.nameview()...)
}

// nameview returns the file name bytes, truncated at the NUL terminator.
func (finfo *FileInfo) nameview() []byte {
	return bstr(finfo.fname[:])
}

// Size returns the size of the file in bytes.
func (finfo *FileInfo) Size() int64 {
	return finfo.fsize
}

// ModTime returns the modification time of the file.
func (finfo *FileInfo) ModTime() time.Time {
	return finfo.datetime.Time()
}

// Mode returns the file mode bits mapped from the FAT attributes: 0666, or
// 0444 if the read-only attribute is set, with ModeDir|0111 added for
// directories. FAT stores no owner/group, so the permission bits are synthetic.
func (finfo *FileInfo) Mode() fs.FileMode {
	m := fs.FileMode(0o666)
	if finfo.fattrib&amRDO != 0 {
		m = 0o444
	}
	if finfo.fattrib&amDIR != 0 {
		m |= fs.ModeDir | 0o111
	}
	return m
}

// Sys returns the raw FAT attribute byte of the directory entry.
func (finfo *FileInfo) Sys() any {
	return finfo.fattrib
}

// IsDir returns true if the file is a directory.
func (finfo *FileInfo) IsDir() bool {
	return finfo.fattrib&amDIR != 0
}
