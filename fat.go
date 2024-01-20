package fat

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"log/slog"
	"math/bits"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"
)

// var _ fs.FS = (*FS)(nil)

const (
	modeRead fs.FileMode = 1 << iota
	modeWrite
)

type accessmode = uint8

const (
	fileaccessRead         accessmode = 0b01
	fileaccessWrite        accessmode = 0b10
	fileaccessOpenExisting accessmode = 0
)

const (
	fileaccessCreateNew accessmode = 1 << (iota + 2)
	fileaccessCreateAlways
	fileaccessOpenAlways
	fileaccessOpenAppend accessmode = 0x30
)

type BlockDevice interface {
	ReadBlocks(dst []byte, startBlock int64) error
	WriteBlocks(data []byte, startBlock int64) error
	EraseSectors(startBlock, numBlocks int64) error
	// Mode returns 0 for no connection/prohibited access, 1 for read-only, 3 for read-write.
	Mode() accessmode
}

// sector index type.
type lba uint32

type FS struct {
	fstype   fstype
	nFATs    uint8
	wflag    uint8  // b0:dirty
	fsi_flag uint8  // FSInfo dirty flag. b7:disabled, b0:dirty.
	nrootdir uint16 // Number of root directory entries.

	blk    blkIdxer
	csize  uint16      // Cluster size in sectors.
	ssize  uint16      // Sector size in bytes.
	lfnbuf [256]uint16 // Long file name working buffer.

	dirbuf    [608]byte // Directory entry block scratchpad for exFAT. (255+44)/15*32 = 608.
	device    BlockDevice
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
	id         uint16 // Filesystem mount ID. Serves to invalidate open files after mount.
	perm       fs.FileMode
}

type objid struct {
	fs      *FS
	id      uint16 // Corresponds to FS.id.
	attr    uint8
	stat    uint8
	objsize int64
	sclust  uint32

	// exFAT only:
	n_cont, n_frag, c_scl, c_size, c_ofs uint32
}

type File struct {
	obj      objid
	flag     uint8
	err      uint8 // abort flag (error code)
	fptr     int64
	clust    uint32
	sect     lba
	dir_sect lba
	dir_ptr  *byte
	cltbl    *uint32   // Pointer to the cluster link map table (Nulled on file open, set by application)
	buf      [512]byte // Private read/write sector buffer.
}

type dir struct {
	obj   objid
	dptr  uint32   // current read/write offset
	clust uint32   // current cluster
	sect  lba      // current sector
	dir   *byte    // current directory entry in win[]
	fn    [12]byte // SFN (in/out) {body[8],ext[3],status[1]}

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

type diskstatus uint8

const (
	diskstatusNoInit diskstatus = 1 << iota
	diskstatusNoDisk
	diskstatusWriteProtected
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
	frUnsupported                        // the operation is not supported
)

func (fr fileResult) Error() string {
	return "fat.fr:" + strconv.Itoa(int(fr))
}

// bootsectorstatus is the return code for mount_volume.
//   - 0:FAT/FAT32 VBR
//   - 1:exFAT VBR
//   - 2:Not FAT and valid BS
//   - 3:Not FAT and invalid BS
//   - 4:Disk error
type bootsectorstatus uint

const (
	bootsectorstatusFAT bootsectorstatus = iota
	bootsectorstatusExFAT
	bootsectorstatusNotFATValidBS
	bootsectorstatusNotFATInvalidBS
	bootsectorstatusDiskError
)

func (fsys *FS) OpenFile(dst *File, name string, mode fs.FileMode) error {
	if dst == nil {
		return frInvalidObject
	}
	return nil
	// var dj dir
	// var cl, bcs, clst, tm uint32
	// var sc lba
	// var ofs int64
	// dj.obj.fs = fsys
	// dj.obj.id = fsys.id

	// return nil, nil
}

// mount initializes the FS with the given BlockDevice.
func (fsys *FS) mount_volume(bd BlockDevice, ssize uint16, mode uint8) (fr fileResult) {
	fsys.fstype = fstypeUnknown // Invalidate any previous mount.
	// From here on out we call mount_volume since we don't care about
	// mutexes or file path handling. File path handling is left to
	// the Go standard library which does a much better job than us.
	// See `filepath` and `fs` standard library packages.
	devMode := fsys.device.Mode()
	if devMode == 0 {
		return frNotReady
	} else if mode&devMode != mode {
		return frDenied
	}
	blk, err := makeBlockIndexer(int(ssize))
	if err != nil {
		return frInvalidParameter
	}
	fsys.blk = blk
	fsys.ssize = ssize
	fmt := fsys.find_volume(0)

	if fmt == bootsectorstatusDiskError {
		return frDiskErr
	} else if fmt == bootsectorstatusNotFATInvalidBS || fmt == bootsectorstatusNotFATValidBS {
		return frNoFilesystem
	}

	if fmt == bootsectorstatusExFAT {
		return fsys.init_exfat()
	}
	return fsys.init_fat()
}

func (fsys *FS) init_fat() fileResult { // Part of mount_volume.
	bsect := fsys.winsect
	ss := fsys.ssize
	if fsys.window_u32(bpbBytsPerSec) != uint32(ss) {
		return frInvalidParameter
	}
	// Number of sectors per FAT.
	fatsize := fsys.window_u32(bpbFATSz16)
	if fatsize == 0 {
		fatsize = fsys.window_u32(bpbFATSz32)
	}
	fsys.fsize = fatsize
	fsys.nFATs = fsys.win[bpbNumFATs]
	if fsys.nFATs != 1 && fsys.nFATs != 2 {
		return frNoFilesystem
	}

	fsys.csize = uint16(fsys.win[bpbSecPerClus])
	if fsys.csize == 0 || (fsys.csize&(fsys.csize-1)) != 0 {
		// Zero or not power of two.
		return frNoFilesystem
	}

	fsys.nrootdir = fsys.window_u16(bpbRootEntCnt)
	if fsys.nrootdir%(ss/32) != 0 {
		// Is not sector aligned.
		return frNoFilesystem
	}

	// Number of sectors on the volume.
	var totalSectors uint32 = uint32(fsys.window_u16(bpbTotSec16))
	if totalSectors == 0 {
		totalSectors = fsys.window_u32(bpbTotSec32)
	}

	// Number of reserved sectors.
	totalReserved := fsys.window_u16(bpbRsvdSecCnt)
	if totalReserved == 0 {
		return frNoFilesystem
	}

	// Determine the FAT subtype. RSV+FAT+DIR
	const sizeDirEntry = 32
	sysect := uint32(totalReserved) + fatsize + uint32(fsys.nrootdir)/(uint32(ss)/sizeDirEntry)
	totalClusters := (totalSectors - sysect) / uint32(fsys.csize)
	if totalSectors < sysect || totalClusters == 0 {
		return frNoFilesystem
	}
	var fmt fstype = fstypeFAT12
	switch {
	case totalClusters > clustMaxFAT32:
		return frNoFilesystem // Too many clusters for FAT32.
	case totalClusters > clustMaxFAT16:
		fmt = fstypeFAT32
	case totalClusters > clustMaxFAT12:
		fmt = fstypeFAT16
	}

	// Boundaries and limits.
	fsys.n_fatent = totalClusters + 2
	fsys.volbase = bsect
	fsys.fatbase = bsect + lba(totalReserved)
	fsys.database = bsect + lba(sysect)
	var sizebFAT uint32
	if fmt == fstypeFAT32 {
		if fsys.window_u16(bpbFSVer32) != 0 {
			return frNoFilesystem // Unsupported FAT subversion, must be 0.0.
		} else if fsys.nrootdir != 0 {
			return frNoFilesystem // Root directory entry count must be 0.
		}
		fsys.dirbase = lba(fsys.window_u32(bpbRootClus32))
		sizebFAT = fsys.n_fatent * 4
	} else {
		if fsys.nrootdir == 0 {
			return frNoFilesystem // Root directory entry count must not be 0.
		}
		fsys.dirbase = fsys.fatbase + lba(fatsize)
		if fmt == fstypeFAT16 {
			sizebFAT = fsys.n_fatent * 2
		} else {
			sizebFAT = fsys.n_fatent*3/2 + fsys.n_fatent&1
		}
	}
	if fsys.fsize < (sizebFAT+uint32(ss-1))/uint32(ss) {
		return frNoFilesystem // FAT size must not be less than FAT sectors.
	}
	// Initialize cluster allocation information for write ops.
	fsys.last_clst = 0xffff_ffff
	fsys.free_clst = 0xffff_ffff
	fsys.fsi_flag = 1 << 7

	// Update FSInfo.
	if fmt == fstypeFAT32 && fsys.window_u16(bpbFSInfo32) == 1 && fsys.move_window(bsect+1) == frOK {
		fsys.fsi_flag = 0
		ok := fsys.window_u16(bs55AA) == 0xaa55 && fsys.window_u32(fsiLeadSig) == 0x41615252 &&
			fsys.window_u32(fsiStrucSig) == 0x61417272
		if ok {
			fsys.free_clst = fsys.window_u32(fsiFree_Count)
			fsys.last_clst = fsys.window_u32(fsiNxt_Free)
		}
	}
	fsys.fstype = fmt // Validate the filesystem.
	fsys.id++         // Increment filesystem ID, invalidates open files.
	return frOK
}

func (fsys *FS) init_exfat() fileResult {
	return frUnsupported // TODO(soypat): implement exFAT.
}

func (fsys *FS) disk_status() diskstatus {
	return 0
}

func (fsys *FS) find_volume(part int64) bootsectorstatus {
	const (
		sizePTE  = 16
		startPTE = 8
	)
	var mbr_pt [4]uint32
	fmt := fsys.check_fs(0)
	if fmt != 2 && (fmt >= 3 || part == 0) {
		// Returns if it is an FAT VBR as auto scan, not a BS or disk error.
		return fmt
	}
	if fsys.win[offsetMBRTable+4] == 0xEE {
		return fsys.find_gpt_volume(part)
	}
	// Read partition table.
	if part > 4 {
		// MBR has 4 partitions max.
		return bootsectorstatusNotFATInvalidBS
	}
	// Read partition table.
	var i uint16
	for i = 0; i < 4; i++ {
		offset := offsetMBRTable + sizePTE*i + startPTE
		mbr_pt[i] = binary.LittleEndian.Uint32(fsys.win[offset:])
	}
	i = 0
	if part > 0 {
		i = uint16(part - 1)
	}
	for {
		fmt = 3
		if mbr_pt[i] > 0 {
			// Check if partition is FAT.
			fmt = fsys.check_fs(lba(mbr_pt[i]))
		}
		i++
		if !(part == 0 && fmt >= 2 && i < 4) {
			break
		}
	}
	return fmt
}

func (fsys *FS) find_gpt_volume(part int64) bootsectorstatus {
	return bootsectorstatusNotFATInvalidBS
}

// check_fs returns:
func (fsys *FS) check_fs(sect lba) bootsectorstatus {
	const (
		offsetJMPBoot   = 0
		offsetSignature = 510
		offsetFileSys32 = 82
	)
	fsys.invalidate_window()
	fr := fsys.move_window(sect)
	if fr != frOK {
		return bootsectorstatusDiskError
	}
	bsValid := 0xaa55 == binary.LittleEndian.Uint32(fsys.win[offsetSignature:])

	if bsValid && !fsys.window_memcmp(offsetJMPBoot, "\xEB\x76\x90EXFAT   ") {
		return bootsectorstatusExFAT // exFAT VBR.
	}
	b := fsys.win[offsetJMPBoot]
	if b != 0xEB && b != 0xE9 && b != 0xE8 {
		// Not a FAT VBR, BS may be valid/invalid.
		return 3 - b2i[bootsectorstatus](bsValid)
	} else if bsValid && fsys.window_memcmp(offsetFileSys32, "FAT32   ") {
		return bootsectorstatusFAT // FAT32 VBR.
	}
	// TODO(soypat): Support early MS-DOS FAT.
	return bootsectorstatusNotFATInvalidBS
}

func (obj *objid) clusterstat(clst uint32) (val uint32) {
	fsys := obj.fs
	if clst < 2 || clst >= fsys.n_fatent {
		return 1 // Internal error
	}
	val = 0xffff_ffff // default  value falls on disk error.
	switch fsys.fstype {
	default:
		// TODO(soypat): implement exFAT.
		return 1 // Not supported.
	case fstypeFAT32:
		sect := fsys.fatbase + lba(fsys.divSS(clst))/2
		ret := fsys.move_window(sect)
		if ret != frOK {
			fsys.logerror("value:move_window", slog.Int("ret", int(ret)))
			break
		}
		val = binary.LittleEndian.Uint32(fsys.win[fsys.modSS(clst*4):])
		val &= mask28bits // FAT32 uses 28bits for cluster address.
	}
	return val
}

func (obj *objid) put_clusterstat(cluster, value uint32) uint32 {
	fsys := obj.fs
	if cluster < 2 || cluster >= fsys.n_fatent {
		return 1 // Internal error
	}
	switch fsys.fstype {
	default:
		return 1 // Not supported.
	case fstypeFAT32, fstypeExFAT: // Similar for both FAT32 and exFAT.
		sect := fsys.fatbase + lba(fsys.divSS(cluster))/4
		ret := fsys.move_window(sect)
		winIdx := fsys.modSS(cluster * 4)
		if ret != frOK {
			break
		} else if fsys.fstype != fstypeExFAT {
			value = (value * mask28bits) |
				(binary.LittleEndian.Uint32(fsys.win[winIdx:]) &^ mask28bits)
		}
		binary.LittleEndian.PutUint32(fsys.win[winIdx:], value)
		fsys.wflag = 1
	}
	return 0
}

func (fsys *FS) move_window(sector lba) (fr fileResult) {
	if sector == fsys.winsect {
		return frOK // Do nothing if window offset not changed.
	}
	fr = fsys.sync_window() // Flush window.
	if fr != frOK {
		return fr
	}
	dr := fsys.disk_read(fsys.win[:], sector, 1)
	if dr != drOK {
		fsys.logerror("move_window:dr", slog.Int("dret", int(dr)))
		sector = badLBA // Invalidate window offset if disk error occured.
		fr = frDiskErr
	}
	fsys.winsect = sector
	return fr
}

func (fsys *FS) invalidate_window() {
	fsys.wflag = 0
	fsys.winsect = badLBA
}

func (fsys *FS) window_memcmp(off uint16, data string) bool {
	return off+uint16(len(data)) <= uint16(len(fsys.win)) && unsafe.String((*byte)(unsafe.Pointer(&fsys.win[off])), len(data)) == data
}

func (fsys *FS) window_u32(off uint16) uint32 {
	fsys.window_boundscheck(off + 4)
	return binary.LittleEndian.Uint32(fsys.win[off:]) // DWORD size.
}

func (fsys *FS) window_u16(off uint16) uint16 {
	fsys.window_boundscheck(off + 2)
	return binary.LittleEndian.Uint16(fsys.win[off:]) // WORD size.
}

func (fsys *FS) window_boundscheck(lim uint16) {
	if lim > uint16(len(fsys.win)) {
		panic("window_boundscheck: out of bounds")
	}
}

func (fsys *FS) sync_window() (fr fileResult) {
	if fsys.wflag == 0 {
		return frOK // Diska access window not dirty.
	}
	ret := fsys.disk_write(fsys.win[:], fsys.winsect, 1)
	if ret != drOK {
		fsys.logerror("sync_window:dw", slog.Int("dret", int(ret)))
		return frDiskErr
	}
	if fsys.nFATs == 2 && fsys.winsect-fsys.fatbase < lba(fsys.fsize) { // Is in 1st FAT?
		// Reflect it to second FAT if needed.
		fsys.disk_write(fsys.win[:], fsys.winsect+lba(fsys.fsize), 1) // Redundancy write, ignore error.
	}
	fsys.wflag = 0
	return frOK
}

func (fsys *FS) disk_write(buf []byte, sector lba, numsectors int) diskresult {
	return drOK
}
func (fsys *FS) disk_read(dst []byte, sector lba, numsectors int) diskresult {
	return drOK
}

func (fsys *FS) dbc_1st(c byte) bool {
	return (fsys.ffCodePage == 0 || fsys.ffCodePage >= 900) &&
		(c >= fsys.dbcTbl[0] || (c >= fsys.dbcTbl[2] && c <= fsys.dbcTbl[3]))
}

func (fsys *FS) dbc_2nd(c byte) bool {
	return (fsys.ffCodePage == 0 || fsys.ffCodePage >= 900) &&
		(c >= fsys.dbcTbl[4] || (c >= fsys.dbcTbl[6] && c <= fsys.dbcTbl[7]) ||
			(c >= fsys.dbcTbl[8] && c <= fsys.dbcTbl[9]))
}

func chk_chr(str *byte, char byte) bool {
	for *str != 0 && *str != char {
		str = (*byte)(unsafe.Add(unsafe.Pointer(str), 1))
	}
	return *str != 0
}

// Sector size divide and modulus. Can each be optimized further on to be a single bitwise instruction.

func (fsys *FS) divSS(n uint32) uint32 { return n / uint32(fsys.ssize) }
func (fsys *FS) modSS(n uint32) uint32 { return n % uint32(fsys.ssize) }

// clst2sect returns the physical sector number from a cluster number.
// Returns 0 if the cluster is invalid.
func (fsys *FS) clst2sect(clst uint32) lba {
	clst -= 2
	if clst >= fsys.n_fatent-2 {
		return 0
	}
	return fsys.database + lba(fsys.csize)*lba(clst)
}
func (fsys *FS) logattrs(level slog.Level, msg string, attrs ...slog.Attr) {}

func (fsys *FS) debug(msg string, attrs ...slog.Attr) {
	fsys.logattrs(slog.LevelDebug, msg, attrs...)
}
func (fsys *FS) info(msg string, attrs ...slog.Attr) {
	fsys.logattrs(slog.LevelInfo, msg, attrs...)
}
func (fsys *FS) warn(msg string, attrs ...slog.Attr) {
	fsys.logattrs(slog.LevelWarn, msg, attrs...)
}
func (fsys *FS) logerror(msg string, attrs ...slog.Attr) {
	fsys.logattrs(slog.LevelError, msg, attrs...)
}

func (dp *dir) follow_path(path string) (fr fileResult) {
	fsys := dp.obj.fs
	path = trimSeparatorPrefix(path)
	dp.obj.sclust = 0 // Set start directory (always root dir)

	dp.obj.n_frag = 0 // Invalidate last fragment counter.
	if fsys.fstype == fstypeExFAT {
		return frUnsupported // TODO(soypat): implement exFAT.
	}
	if len(path) == 0 || isTermLFN(path[0]) {
		// Received origin directory.
		dp.fn[nsFLAG] = nsNONAME
		return dp.sdi(0)
	}

	for {

	}

}

func (dp *dir) create_name(path string) fileResult {
	var (
		p    = path
		fsys = dp.obj.fs
		lfn  = fsys.lfnbuf[:]
		di   = 0
	)
	var wc uint16
	for {
		uc, plen := utf8.DecodeRuneInString(p)
		if uc == utf8.RuneError {
			return frInvalidName
		} else if plen > 2 {
			// Store high surrogate.
			lfn[di] = uint16(uc >> 16)
			di++
		}
		p = p[plen:]
		wc = uint16(uc)
		if isTermLFN(wc) || isSep(wc) {
			break // Break on end of path or a separator.
		}
		if strings.IndexByte("*:<>|\"?\x7f", byte(wc)) >= 0 {
			return frInvalidName
		}
		if di >= lfnBufSize {
			return frInvalidName
		}
		lfn[di] = wc
		di++
	}
	var cf byte
	if isTermLFN(wc) {
		cf = nsLAST // Stopped at last segment (end of path).
	} else {
		p = trimSeparatorPrefix(p)
		if len(p) > 0 && isTermLFN(p[0]) {
			cf = nsLAST
		}
	}
	path = p // return pointer to next segment (??)

	for di > 0 {
		wc = lfn[di-1]
		if wc != ' ' && wc != '.' {
			break
		}
		di--
	}
	lfn[di] = 0
	if di == 0 {
		return frInvalidName // Reject null name.
	}
	var si int
	for si = 0; si < di && lfn[si] == ' '; si++ {
	}
	if si > 0 || lfn[si] == '.' {
		cf |= nsLOSS | nsLFN // Leading dot.
	}
	for di > 0 && lfn[di-1] != '.' {
		di-- // Find last dot (di<=si: no extension).
	}
	for i := 0; i < 11; i++ {
		dp.fn[i] = ' ' // memset(dp.fn, ' ', 11);
	}

	i := 0
	b := byte(0)
	ni := 8
	for {
		wc = lfn[si]
		si++
		if wc == 0 {
			break
		}
		if wc == ' ' || (wc == '.' && si != di) {
			cf |= nsLOSS | nsLFN // Remove embedded spaces and dots.
			continue
		}
		if i >= ni || si == di {
			if ni == 11 {
				cf |= nsLOSS | nsLFN // Possible name extension overflow.
				break
			}
			if si != di {
				cf |= nsLOSS | nsLFN // Possible name body overflow.
			}
			if si > di {
				break
			}
			si = di
			i = 8
			ni = 11
			b <<= 2
			continue
		}

		if wc >= 0x80 {
			// Possible extended character.
			cf |= nsLFN // Flag LFN entry needs creation.

		}
	}
	return frUnsupported
}

func (dp *dir) sdi(ofs uint32) fileResult {
	fsys := dp.obj.fs
	switch {
	case fsys.fstype == fstypeExFAT && ofs >= maxDIREx:
		return frIntErr
	case ofs >= maxDIR:
		return frIntErr
	}
	dp.dptr = ofs
	clst := dp.obj.sclust
	if clst == 0 {
		if fsys.fstype >= fstypeFAT32 {
			clst = uint32(fsys.dirbase)
			dp.obj.stat = 0 // exFAT: Root dir has a FAT chain.
		}

	}

	if clst == 0 {
		if ofs/sizeDirEntry >= uint32(fsys.nrootdir) {
			return frIntErr
		}
		dp.sect = fsys.dirbase
	} else {
		// Bytes per cluster.
		csz := uint32(fsys.csize) * uint32(fsys.ssize)
		for ofs >= csz {
			clst = dp.obj.clusterstat(clst)
			if clst == 0xffff_ffff {
				return frDiskErr
			} else if clst < 2 || clst >= fsys.n_fatent {
				return frIntErr
			}
			ofs -= csz
		}
		dp.sect = fsys.clst2sect(clst)
	}

	dp.clust = clst
	if dp.sect == 0 {
		return frIntErr
	}
	dp.sect += lba(fsys.divSS(ofs))
	dp.dir = &fsys.win[fsys.modSS(ofs)]
	return frOK
}

// blkIdxer is a helper for calculating block indexes and offsets.
type blkIdxer struct {
	blockshift int64
	blockmask  int64
}

func makeBlockIndexer(blockSize int) (blkIdxer, error) {
	if blockSize <= 0 {
		return blkIdxer{}, errors.New("blockSize must be positive and non-zero")
	}
	tz := bits.TrailingZeros(uint(blockSize))
	if blockSize>>tz != 1 {
		return blkIdxer{}, errors.New("blockSize must be a power of 2")
	}
	blk := blkIdxer{
		blockshift: int64(tz),
		blockmask:  (1 << tz) - 1,
	}
	return blk, nil
}

// size returns the size of a block in bytes.
func (blk *blkIdxer) size() int64 {
	return 1 << blk.blockshift
}

// off gets the offset of the byte at byteIdx from the start of its block.
//
//go:inline
func (blk *blkIdxer) off(byteIdx int64) int64 {
	return blk._moduloBlockSize(byteIdx)
}

// idx gets the block index that contains the byte at byteIdx.
//
//go:inline
func (blk *blkIdxer) idx(byteIdx int64) int64 {
	return blk._divideBlockSize(byteIdx)
}

// modulo and divide are defined in terms of bit operations for speed since
// blockSize is a power of 2.

//go:inline
func (blk *blkIdxer) _moduloBlockSize(n int64) int64 { return n & blk.blockmask }

//go:inline
func (blk *blkIdxer) _divideBlockSize(n int64) int64 { return n >> blk.blockshift }

type _integer interface {
	~uint8 | ~uint16 | ~uint32 | ~int | ~uint
}

func b2i[T _integer](b bool) T {
	if b {
		return 1
	}
	return 0
}

func trimSeparatorPrefix(s string) string {
	for len(s) > 0 && isSep(s[0]) {
		s = s[1:]
	}
	return s
}

func trimChar(s string, char byte) string {
	for len(s) > 0 && s[0] == char {
		s = s[1:]
	}
	return s
}

func isUpper(c byte) bool            { return 'A' <= c && c <= 'Z' }
func isLower(c byte) bool            { return 'a' <= c && c <= 'z' }
func isDigit(c byte) bool            { return '0' <= c && c <= '9' }
func isSep[T _integer](c T) bool     { return c == '/' || c == '\\' }
func isTermLFN[T _integer](c T) bool { return c < ' ' }
func isSurrogate(c uint16) bool      { return c >= 0xd800 && c <= 0xdfff }
func isSurrogateH(c uint16) bool     { return c >= 0xd800 && c <= 0xdbff }
func isSurrogateL(c uint16) bool     { return c >= 0xdc00 && c <= 0xdfff }

// func isTermNoLFN(c byte) bool { return c < '!' }
