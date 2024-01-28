package fat

import (
	"errors"
	"io"
	"math"
	"time"
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

	allowedModes = ModeRead | ModeWrite | ModeCreateNew | ModeCreateAlways | ModeOpenExisting | ModeOpenAppend
)

var (
	errInvalidMode   = errors.New("invalid fat access mode")
	errForbiddenMode = errors.New("forbidden fat access mode")
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

// Read reads up to len(buf) bytes from the File. It implements the [io.Reader] interface.
func (fp *File) Read(buf []byte) (int, error) {
	fr := fp.obj.validate()
	if fr != frOK {
		return 0, fr
	}
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
	fr := fp.obj.validate()
	if fr != frOK {
		return 0, fr
	}
	bw, fr := fp.f_write(buf)
	if fr != frOK {
		return bw, fr
	}
	return bw, nil
}

// Close closes the file and syncs any unwritten data to the underlying device.
func (fp *File) Close() error {
	fr := fp.obj.validate()
	if fr != frOK {
		return fr
	}

	fr = fp.f_close()
	if fr != frOK {
		return fr
	}
	return nil
}

// Sync commits the current contents of the file to the filesystem immediately.
func (fp *File) Sync() error {
	fr := fp.obj.validate()
	if fr != frOK {
		return fr
	}

	fr = fp.obj.fs.sync()
	if fr != frOK {
		return fr
	}
	return nil
}

// Mode returns the lowest 2 bits of the file's permission (read, write or both).
func (fp *File) Mode() Mode {
	return Mode(fp.flag & 3)
}

// OpenDir opens the named directory for reading.
func (fsys *FS) OpenDir(dp *Dir, path string) error {
	fr := fsys.f_opendir(&dp.dir, path)
	if fr != frOK {
		return fr
	}
	return nil
}

// ForEachFile calls the callback function for each file in the directory.
func (dp *Dir) ForEachFile(callback func(*FileInfo) error) error {
	fr := dp.obj.validate()
	if fr != frOK {
		return fr
	} else if dp.obj.fs.perm&ModeRead == 0 {
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

// AlternateName returns the alternate name of the file.
func (finfo *FileInfo) AlternateName() string {
	return str(finfo.altname[:])
}

// Name returns the name of the file.
func (finfo *FileInfo) Name() string {
	return str(finfo.fname[:])
}

// Size returns the size of the file in bytes.
func (finfo *FileInfo) Size() int64 {
	return finfo.fsize
}

// ModTime returns the modification time of the file.
func (finfo *FileInfo) ModTime() time.Time {
	// https://www.win.tue.nl/~aeb/linux/fs/fat/fat-1.html
	hour := int(finfo.ftime >> 11)
	min := int((finfo.ftime >> 5) & 0x3f)
	doubleSeconds := int(finfo.ftime & 0x1f)
	yearSince1980 := int(finfo.fdate >> 9)
	month := int((finfo.fdate >> 5) & 0xf)
	day := int(finfo.fdate & 0x1f)
	return time.Date(yearSince1980+1980, time.Month(month), day, hour, min, 2*doubleSeconds, 0, time.UTC)
}

// IsDir returns true if the file is a directory.
func (finfo *FileInfo) IsDir() bool {
	return finfo.fattrib&amDIR != 0
}
