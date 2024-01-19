package fat

import (
	"encoding/binary"
	"log/slog"
	"unsafe"
)

// sector index type.
type lba uint32

type FS struct {
	fstype   fstype
	nFATs    uint8
	wflag    uint8 // b0:dirty
	nrootdir uint16

	csize  uint16 // Cluster size in sectors.
	ssize  uint16 // Sector size in bytes.
	lfnbuf []byte // Long file name working buffer.

	dirbuf []byte // Directory entry block scratchpad for exFAT.

	last_clst uint32 // Last allocated clusters.
	free_clst uint32 // Number of free clusters.

	// No relative pathing, we can always use a [fs.FS] wrapper.

	n_fatent uint32 // Number of FAT entries (= number of clusters + 2)
	fsize    uint32 // Number of sectors per FAT.

	volbase  lba // Volume base sector.
	fatbase  lba // FAT base sector.
	dirbase  lba // Root directory base sector/cluster.
	database lba // Data base sector.

	bitbase lba // Allocation bitmap base sector (exFAT only)

	winsect    lba       // Current sector appearing in the win[].
	win        [512]byte // Disk access window for Directory/FAT/File.
	ffCodePage int
	dbcTbl     [10]byte
}

type objid struct {
	fs      *FS
	id      uint16
	attr    uint8
	stat    uint8
	objsize int64
	sclust  uint32

	// exFAT only:
	n_cont, n_frag, c_scl, c_size, c_ofs uint32
}

type file struct {
	obj      objid
	flag     uint8
	err      uint8 // abort flag (error code)
	fptr     int64
	clust    uint32
	sect     lba
	dir_sect lba
	dir_ptr  *byte
	cltbl    *uint32 // Pointer to the cluster link map table (Nulled on file open, set by application)
}

type dir struct {
	obj   objid
	dptr  uint32   // current read/write offset
	clust uint32   // current cluster
	sect  lba      // current sector
	dir   *byte    // current directory entry in win[]
	fn    [12]byte // Pointer to the SFN (in/out)

	// Use LFN:
	blk_ofs uint32 // Offset of current entry block being processed (0:sfn, 1-:lfn)
}

const (
	lfnBufSize = 255
	sfnBufSize = 12
)

type fileinfo struct {
	fsize   int64 // File Size.
	fdate   uint16
	ftime   uint16
	altname [sfnBufSize + 1]byte
	fname   [lfnBufSize + 1]byte
}

type mkfsParam struct {
	fmt     byte
	n_fat   uint8
	align   uint32
	n_root  uint32
	au_size uint32 // cluster size in bytes.
}

type fstype byte

const (
	fstypeUnknown fstype = iota
	fstypeFAT12
	fstypeFAT16
	fstypeFAT32
	fstypeExFAT
)

type diskresult int

const (
	drOK             diskresult = iota // successful
	drError                            // R/W error
	drWriteProtected                   // write protected
	drNotReady                         // not ready
	drParError                         // invalid parameter
)

// fileResult is file function return code.
type fileResult int

const (
	frOK               fileResult = iota // succeeded
	frDiskErr                            // a hard error occurred in the low level disk I/O layer
	frIntErr                             // assertion failed
	frNotReady                           // the physical drive cannot work
	frNoFile                             // could not find the file
	frNoPath                             // could not find the path
	frInvalidName                        // the path name format is invalid
	frDenied                             // access denied due to prohibited access or directory full
	frExist                              // access denied due to prohibited access
	frInvalidObject                      // the file/directory object is invalid
	frWriteProtected                     // the physical drive is write protected
	frInvalidDrive                       // the logical drive number is invalid
	frNotEnabled                         // the volume has no work area
	frNoFilesystem                       // there is no valid FAT volume
	frMkfsAborted                        // the f_mkfs() aborted due to any problem
	frTimeout                            // could not get a grant to access the volume within defined period
	frLocked                             // the operation is rejected according to the file sharing policy
	frNotEnoughCore                      // LFN working buffer could not be allocated
	frTooManyOpenFiles                   // number of open files > FF_FS_LOCK
	frInvalidParameter                   // given parameter is invalid
)

func (obj *objid) clusterstat(clst uint32) (val uint32) {
	fs := obj.fs
	if clst < 2 || clst >= fs.n_fatent {
		return 1 // Internal error
	}
	val = 0xffff_ffff // default  value falls on disk error.
	switch fs.fstype {
	default:
		return 1 // Not supported.
	case fstypeFAT32:
		sect := fs.fatbase + lba(fs.divSS(clst))/2
		ret := fs.move_window(sect)
		if ret != frOK {
			fs.logerror("value:move_window", slog.Int("ret", int(ret)))
			break
		}
		val = binary.LittleEndian.Uint32(fs.win[fs.modSS(clst*4):])
		val &= mask28bits // FAT32 uses 28bits for cluster address.
	}
	return val
}

func (obj *objid) put_clusterstat(cluster, value uint32) uint32 {
	fs := obj.fs
	if cluster < 2 || cluster >= fs.n_fatent {
		return 1 // Internal error
	}
	switch fs.fstype {
	default:
		return 1 // Not supported.
	case fstypeFAT32, fstypeExFAT: // Identical for both FAT32 and exFAT.
		sect := fs.fatbase + lba(fs.divSS(cluster))/4
		ret := fs.move_window(sect)
		winIdx := fs.modSS(cluster * 4)
		if ret != frOK {
			break
		} else if fs.fstype != fstypeExFAT {
			value = (value * mask28bits) |
				(binary.LittleEndian.Uint32(fs.win[winIdx:]) &^ mask28bits)
		}
		binary.LittleEndian.PutUint32(fs.win[winIdx:], value)
		fs.wflag = 1
	}
	return 0
}

func (fs *FS) move_window(sector lba) (fr fileResult) {
	if sector == fs.winsect {
		return frOK // Do nothing if window offset not changed.
	}
	fr = fs.sync_window() // Flush window.
	if fr != frOK {
		return fr
	}
	dr := fs.disk_read(fs.win[:], sector, 1)
	if dr != drOK {
		fs.logerror("move_window:dr", slog.Int("dret", int(dr)))
		sector = badLBA // Invalidate window offset if disk error occured.
		fr = frDiskErr
	}
	fs.winsect = sector
	return fr
}

func (fs *FS) sync_window() (fr fileResult) {
	if fs.wflag == 0 {
		return frOK // Diska access window not dirty.
	}
	ret := fs.disk_write(fs.win[:], fs.winsect, 1)
	if ret != drOK {
		fs.logerror("sync_window:dw", slog.Int("dret", int(ret)))
		return frDiskErr
	}
	if fs.nFATs == 2 && fs.winsect-fs.fatbase < lba(fs.fsize) { // Is in 1st FAT?
		// Reflect it to second FAT if needed.
		fs.disk_write(fs.win[:], fs.winsect+lba(fs.fsize), 1) // Redundancy write, ignore error.
	}
	fs.wflag = 0
	return frOK
}

func (fs *FS) disk_write(buf []byte, sector lba, numsectors int) diskresult {
	return drOK
}
func (fs *FS) disk_read(dst []byte, sector lba, numsectors int) diskresult {
	return drOK
}

func (fs *FS) dbc_1st(c byte) bool {
	return (fs.ffCodePage == 0 || fs.ffCodePage >= 900) &&
		(c >= fs.dbcTbl[0] || (c >= fs.dbcTbl[2] && c <= fs.dbcTbl[3]))
}

func (fs *FS) dbc_2nd(c byte) bool {
	return (fs.ffCodePage == 0 || fs.ffCodePage >= 900) &&
		(c >= fs.dbcTbl[4] || (c >= fs.dbcTbl[6] && c <= fs.dbcTbl[7]) ||
			(c >= fs.dbcTbl[8] && c <= fs.dbcTbl[9]))
}

func chk_chr(str *byte, char byte) bool {
	for *str != 0 && *str != char {
		str = (*byte)(unsafe.Add(unsafe.Pointer(str), 1))
	}
	return *str != 0
}

// Sector size divide and modulus. Can each be optimized further on to be a single bitwise instruction.

func (ds *FS) divSS(n uint32) uint32 { return n / uint32(ds.ssize) }
func (ds *FS) modSS(n uint32) uint32 { return n % uint32(ds.ssize) }

func (ds *FS) logattrs(level slog.Level, msg string, attrs ...slog.Attr) {}

func (ds *FS) debug(msg string, attrs ...slog.Attr) {
	ds.logattrs(slog.LevelDebug, msg, attrs...)
}
func (ds *FS) info(msg string, attrs ...slog.Attr) {
	ds.logattrs(slog.LevelInfo, msg, attrs...)
}
func (ds *FS) warn(msg string, attrs ...slog.Attr) {
	ds.logattrs(slog.LevelWarn, msg, attrs...)
}
func (ds *FS) logerror(msg string, attrs ...slog.Attr) {
	ds.logattrs(slog.LevelError, msg, attrs...)
}
