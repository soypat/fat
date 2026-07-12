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

// FormatParams returns the parameters describing the mounted volume, as would
// be passed to [Formatter.Format] to recreate it. The block size is not part of
// FormatParams; get it from [FS.BlockSize].
//
// Label is read from the volume label entry of the root directory and is empty
// when the volume has none. [Formatter.Format] does not write one, so a volume
// only has a label if it was set by another tool.
func (fsys *FS) FormatParams() (FormatParams, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	label, fr := fsys.f_getlabel(nil)
	if fr != frOK {
		return FormatParams{}, fr
	}
	return FormatParams{
		Label:       string(label),
		Format:      fsys.fstype,
		ClusterSize: int(fsys.csize),
	}, nil
}

// BlockSize returns the block (sector) size in bytes of the mounted volume, as
// would be passed to [Formatter.Format]. It is zero if not mounted.
func (fsys *FS) BlockSize() int {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return int(fsys.ssize)
}

// FSConfig holds the behavioral choices an FS makes that the FAT format itself
// does not decide. It may be set at any time; the zero value is the default.
type FSConfig struct {
	// NoZeroFilling leaves the gap created by growing a file past its end
	// uninitialized, which is what FatFs does and what f_lseek documents ("the
	// contents of the expanded area are undefined").
	//
	// Understand what the gap is before setting this. FAT has no sparse files: a
	// file is a size plus a chain of clusters, and every byte inside the size is
	// file content. Growing a file links clusters into the chain and raises the
	// size — it does not touch their data sectors. So the "gap" is not a hole, it
	// is ordinary file content that nobody wrote, and it reads back as whatever
	// was last on the media. On a volume that has ever deleted a file, that is
	// the deleted file's contents, handed to a caller who never wrote them and
	// was never given them.
	//
	// By default this package zero-fills that gap, as POSIX and the Windows FAT
	// driver do. The cost is real — the gap is written, so a Seek far past EOF
	// followed by a Write now costs the bytes it skipped — and that cost is why
	// FatFs declined to pay it on an 8-bit micro. Set this to get FatFs' bytes
	// back, exactly: for byte-for-byte compatibility with a reference image, or
	// when the write bandwidth matters more than what the gap discloses.
	NoZeroFilling bool
}

// Configure applies cfg to the filesystem. It may be called before or after
// Mount, and affects only subsequent operations.
func (fsys *FS) Configure(cfg FSConfig) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.noZeroFill = cfg.NoZeroFilling
}

// zeros is the source for zero-filling a gap. It is read-only and shared:
// f_write never modifies the buffer it is given.
var zeros [512]byte

// growTo brings FatFs' file pointer to ofs, extending the file if ofs is past
// the end of it. This is where a position that outran the file becomes real.
//
// The extension is zero-filled unless [FSConfig.NoZeroFilling] is set: FatFs grows
// a file by linking clusters and never erasing them, so the bytes in between
// would otherwise be whatever the media last held. Writing zeros through f_write
// allocates exactly the same clusters FatFs' own stretch would, and initializes
// them on the way past.
func (fp *File) growTo(ofs int64) fileResult {
	fsys := fp.obj.fs
	if ofs <= fp.obj.objsize {
		return fp.f_lseek(ofs) // Nothing to grow; the data is already there.
	}
	if fp.flag&faWrite == 0 {
		return frDenied // Only a writable handle can extend a file.
	}
	if fsys.noZeroFill {
		return fp.f_lseek(ofs) // FatFs: stretch the chain and leave the gap as it lies.
	}
	if fr := fp.f_lseek(fp.obj.objsize); fr != frOK {
		return fr
	}
	for fp.fptr < ofs {
		n := min(int64(len(zeros)), ofs-fp.fptr)
		bw, fr := fp.f_write(zeros[:n])
		if fr != frOK {
			return fr
		} else if int64(bw) < n {
			return frDenied // The device filled up before the gap was covered.
		}
	}
	return frOK
}

// AppendLabel appends the volume label of the mounted filesystem to dst and
// returns the extended buffer. It appends nothing if the volume has no label
// entry in its root directory.
func (fsys *FS) AppendLabel(dst []byte) ([]byte, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	label, fr := fsys.f_getlabel(dst)
	if fr != frOK {
		return dst, fr
	}
	return label, nil
}

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
	if fp.pos > fp.obj.objsize {
		// The position was seeked past the end. There is nothing there to read, and
		// it must not be reached by seeking FatFs' pointer to it: that would clip
		// the pointer on a read-only handle and grow the file on a writable one.
		if fp.flag&faRead == 0 {
			return 0, frDenied
		}
		return 0, io.EOF
	}
	if fr = fp.f_lseek(fp.pos); fr != frOK {
		return 0, fr
	}
	br, fr := fp.f_read(buf)
	fp.pos = fp.fptr
	if fr != frOK {
		return br, fr
	} else if br == 0 {
		return br, io.EOF
	}
	return br, nil
}

// Write writes len(buf) bytes to the File. It implements the [io.Writer] interface.
//
// Writing at a position past the end of the file extends it, and the bytes
// skipped over are zero-filled — see [FSConfig.NoZeroFilling] for what that costs
// and how to turn it off.
func (fp *File) Write(buf []byte) (int, error) {
	fsys, fr := fp.lock()
	if fr != frOK {
		return 0, fr
	}
	defer fsys.mu.Unlock()
	// Make the position real before writing at it: it may be past the end of the
	// file, in which case the gap in between has to be allocated and filled.
	if fr = fp.growTo(fp.pos); fr != frOK {
		return 0, fr
	}
	bw, fr := fp.f_write(buf)
	fp.pos = fp.fptr
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
		return 0, io.EOF
	}
	cur := fp.pos
	if fr = fp.f_lseek(off); fr != frOK {
		return 0, fr
	}
	n, fr := fp.f_read(p)
	// Restoring the position costs nothing: it is a number, and FatFs' pointer is
	// brought back into line lazily by whatever runs next.
	fp.pos = cur
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
// end of the file extends it, zero-filling the gap; see [FSConfig.NoZeroFilling].
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
	cur := fp.pos
	if fr = fp.growTo(off); fr != frOK {
		return 0, fr
	}
	n, fr := fp.f_write(p)
	fp.pos = cur
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
// end when shrinking and extending it with zeros when growing (see
// [FSConfig.NoZeroFilling]). The file must be open for writing.
//
// It does not move the file offset, as POSIX ftruncate does not. An offset left
// beyond the new end of the file stays there, and a write at it will extend the
// file back out.
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
	cur := fp.pos
	// f_truncate cuts the file at FatFs' pointer — it takes no size — so the
	// pointer has to be put where the new end belongs. growTo does that, and
	// allocates and fills the extension if the new end is past the old one.
	fr = fp.growTo(size)
	if fr == frOK {
		fr = fp.f_truncate()
	}
	if fr != frOK {
		return fr
	}
	// The offset is a number, not a cursor into the data, so it survives the file
	// shrinking out from under it. This is the whole reason File carries pos: with
	// only FatFs' pointer to work with, restoring an offset beyond the new size
	// would mean seeking there, and seeking there on a writable handle would grow
	// the file straight back and undo the truncate.
	fp.pos = cur
	return nil
}

// Seek sets the offset for the next Read or Write on the file to offset,
// interpreted according to whence: [io.SeekStart], [io.SeekCurrent] or
// [io.SeekEnd]. It returns the new offset and implements the [io.Seeker]
// interface.
//
// Seeking past the end of the file is legal and changes nothing on the device:
// the file does not grow, no clusters are allocated, and the seek cannot fail
// for want of space. The gap appears only when something writes into it, and a
// Read before then returns io.EOF. This is what os.File does.
//
// It is not what FatFs does. f_lseek will not let its pointer exceed the file
// size, so it extends the file on a writable handle and silently clips the seek
// back to the end on a read-only one — a caller who seeks to 48 in an empty file
// is told it is at 0. Neither happens here; see File.pos.
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
		abs = fp.pos + offset
	case io.SeekEnd:
		abs = fp.obj.objsize + offset
	default:
		return 0, errWhence
	}
	if abs < 0 {
		return 0, errNegativeSeek
	}
	if abs <= fp.obj.objsize {
		// Within the file: move FatFs' pointer now, so a seek to an unreachable
		// offset still reports the disk error that finding it produced.
		if fr = fp.f_lseek(abs); fr != frOK {
			return 0, fr
		}
	}
	fp.pos = abs
	return abs, nil
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
	if fsys.fstype == _FormatUnknown {
		return frNoFilesystem // Not mounted.
	}
	var fr fileResult = frOK
	if fsys.perm&ModeWrite != 0 {
		fr = fsys.sync()
	}
	fsys.fstype = _FormatUnknown // Invalidate the filesystem object.
	fsys.id++                    // Invalidate open files and directories.
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
