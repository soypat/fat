package fat

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"math/bits"
	"runtime"
	"strings"
	"unicode/utf8"
	"unsafe"
)

// var _ fs.FS = (*FS)(nil)

type BlockDevice interface {
	ReadBlocks(dst []byte, startBlock int64) (int, error)
	WriteBlocks(data []byte, startBlock int64) (int, error)
	EraseBlocks(startBlock, numBlocks int64) error
	// Mode returns 0 for no connection/prohibited access, 1 for read-only, 3 for read-write.
	// Mode() accessmode
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
	perm       Mode
	codepage   []byte // unicode conversion table.
	exCvt      []byte //  points to _tblCT* corresponding to codepage table.
	log        *slog.Logger
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
	err      fileResult // abort flag (error code)
	fptr     int64
	clust    uint32
	sect     lba
	dir_sect lba
	dir_ptr  []byte
	cltbl    []uint32  // Pointer to the cluster link map table (Nulled on file open, set by application)
	buf      [512]byte // Private read/write sector buffer.
}

type dir struct {
	obj   objid
	dptr  uint32   // current read/write offset
	clust uint32   // current cluster
	sect  lba      // current sector
	dir   []byte   // current directory entry in win[]
	fn    [12]byte // SFN (in/out) {body[8],ext[3],status[1]}

	// Use LFN:
	blk_ofs uint32 // Offset of current entry block being processed (0:sfn, 1-:lfn)
}

const (
	lfnBufSize = 255
	sfnBufSize = 12
)

type FileInfo struct {
	fsize   int64 // File Size.
	fdate   uint16
	ftime   uint16
	fattrib byte
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
	frClosed                             // the file is closed
	frGeneric                            // fat generic error
)

func (fr fileResult) Error() string {
	return fr.String()
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

func (fp *File) f_read(buff []byte) (br int, res fileResult) {
	fsys := fp.obj.fs
	fsys.trace("f_read", slog.Int("len", len(buff)))
	rbuff := buff
	if fp.flag&faRead == 0 {
		return 0, frDenied
	} else if fsys.perm&ModeRead == 0 {
		return 0, frDenied
	}
	remain := fp.obj.objsize - fp.fptr
	btr := len(buff)
	if btr > int(remain) {
		btr = int(remain)
	}
	var csect, clst uint32
	var rcnt int
	ss := int64(fsys.ssize)
	cs := int64(fsys.csize)

	for {
		btr -= rcnt
		br += rcnt
		rbuff = rbuff[rcnt:]
		fp.fptr += int64(rcnt)
		if btr <= 0 {
			break
		}
		if fp.fptr%ss == 0 {
			csect = uint32((fp.fptr / ss) & (cs - 1))
			if csect == 0 {
				if fp.fptr == 0 {
					clst = fp.obj.sclust
				} else {
					// Follow cluster chain on the FAT.
					clst = fp.obj.clusterstat(fp.clust)
				}
				if clst < 2 {
					return br, fp.abort(frIntErr)
				} else if clst == maxu32 {
					return br, fp.abort(frDiskErr)
				}
				fp.clust = clst
			}
			sect := fsys.clst2sect(fp.clust)
			if sect == 0 {
				return br, fp.abort(frIntErr)
			}
			sect += lba(csect)
			cc := btr / int(ss)
			if cc > 0 {
				// When remaining bytes >= sector size, read maximum contiguous sectors directly.
				if csect+uint32(cc) > uint32(cs) {
					// Clip at cluster boundary.
					cc = int(cs) - int(csect)
				}
				if fsys.disk_read(buff[br:], sect, cc) != drOK {
					return br, fp.abort(frDiskErr)
				}
				if fp.flag&faDIRTY != 0 && fp.sect-sect < lba(cc) {
					off := (fp.sect - sect) * lba(ss)
					copy(rbuff[off:], fp.buf[:])
				}
				// Number of bytes transferred.
				rcnt = int(ss) * cc
				continue
			}
			if fp.flag&faDIRTY != 0 {
				// Write back dirty cache.
				if fsys.disk_write(fp.buf[:], fp.sect, 1) != drOK {
					return br, fp.abort(frDiskErr)
				}
				fp.flag &^= faDIRTY
			}
			if fsys.disk_read(fp.buf[:], sect, 1) != drOK {
				return br, fp.abort(frDiskErr)
			}
			fp.sect = sect
		}
		modfptr := int(fp.fptr % ss)
		rcnt = int(ss) - modfptr
		if rcnt > btr {
			rcnt = btr
		}
		copy(rbuff[:rcnt], fp.buf[modfptr:])
	}
	return br, frOK
}

func (fp *File) f_close() fileResult {
	fsys := fp.obj.fs
	fsys.trace("f_close")
	fr := fsys.f_sync(fp)
	if fr != frOK {
		return fr
	} else if fr = fp.obj.validate(); fr != frOK {
		return fr
	}
	fp.obj.fs = nil
	return frOK
}

func (obj *objid) validate() fileResult {
	if obj.fs == nil || obj.id != obj.fs.id {
		return frInvalidObject
	}
	return frOK
}

func (fsys *FS) f_sync(fp *File) (fr fileResult) {
	fsys.trace("f_sync")
	if fp.flag&faMODIFIED == 0 {
		return frOK // No pending changes to file.
	} else if fsys.fstype == fstypeExFAT {
		return frUnsupported
	}
	if fp.flag&faDIRTY != 0 {
		if fsys.disk_write(fp.buf[:], fp.sect, 1) != drOK {
			return frDiskErr
		}
		fp.flag &^= faDIRTY
	}

	// Update directory entry.
	tm := fsys.time()
	// TODO(soypat): implement exFAT here.
	fr = fsys.move_window(fp.dir_sect)
	if fr != frOK {
		return fr
	}
	dir := fp.dir_ptr
	dir[dirAttrOff] = amARC // 'file changed' attribute set.
	fsys.st_clust(dir, fp.obj.sclust)
	binary.LittleEndian.PutUint32(dir[dirFileSizeOff:], uint32(fp.obj.objsize))
	binary.LittleEndian.PutUint32(dir[dirModTimeOff:], tm)
	binary.LittleEndian.PutUint32(dir[dirLstAccDateOff:], 0)
	fsys.wflag = 1
	fr = fsys.sync()
	fp.flag &^= faMODIFIED
	return fr
}

func (fp *File) abort(fr fileResult) fileResult {
	fp.err = fr
	return fr
}

func (fp *File) f_write(buf []byte) (bw int, fr fileResult) {
	fr = fp.obj.validate()
	if fr != frOK {
		return 0, fr
	} else if fp.err != frOK {
		return 0, fp.err
	} else if fp.flag&faWrite == 0 {
		return 0, frWriteProtected
	} else if fp.obj.fs.fstype == fstypeExFAT {
		return 0, frUnsupported
	} else if fp.obj.fs.perm&ModeWrite == 0 {
		return 0, frWriteProtected
	}
	fs := fp.obj.fs
	btw := len(buf)
	if fs.fstype != fstypeExFAT && fp.fptr+int64(btw) < fp.fptr {
		// Make sure file does not reach over 4GB limit on non exFAT.
		btw = int(int64(maxu32) - fp.fptr)
	}

	wbuff := buf
	var wcnt int
	var clst uint32
outerLoop:
	for btw > 0 {
		btw -= wcnt
		bw += wcnt
		wbuff = wbuff[wcnt:]
		fp.fptr += int64(wcnt)
		fp.obj.objsize = max(fp.obj.objsize, fp.fptr)

		if fs.modSS(uint32(fp.fptr)) == 0 {
			// On the sector boundary?
			csect := uint32(fp.fptr/int64(fs.ssize)) & uint32(fs.csize-1)
			if csect == 0 {
				if fp.fptr == 0 {
					clst = fp.obj.sclust
					if clst == 0 {
						// No cluster allocated yet.
						clst = fp.obj.create_chain(0)
					}
				} else {
					// Middle or end of file.
					clst = fp.obj.create_chain(fp.clust)
				}
				switch clst {
				case 0:
					// Could not allocate a new cluster (disk full)
					break outerLoop
				case 1:
					return bw, fp.abort(frIntErr)
				case maxu32:
					return bw, fp.abort(frDiskErr)
				}
				fp.clust = clst // Update current cluster.
				if fp.obj.sclust == 0 {
					// Set cluster if is the first write.
					fp.obj.sclust = clst
				}
			}
			if fp.flag&faDIRTY != 0 {
				// Write-back sector cache if needed.
				if fs.disk_write(fp.buf[:], fp.sect, 1) != drOK {
					return bw, fp.abort(frDiskErr)
				}
				fp.flag &^= faDIRTY
			}
			sect := fs.clst2sect(fp.clust)
			if sect == 0 {
				return bw, fp.abort(frIntErr)
			}
			sect += lba(csect)
			cc := fs.divSS(uint32(btw))
			if cc > 0 {
				if csect+cc > uint32(fs.csize) {
					cc = uint32(fs.csize) - csect // clip at cluster boundary.
				}
				if fs.disk_write(wbuff[:cc*uint32(fs.ssize)], sect, int(cc)) != drOK {
					return bw, fp.abort(frDiskErr)
				}
				off := fp.sect - sect
				if off < lba(cc) {
					// Refill sector cache if it gets invalidated by the disk_write().
					copy(fp.buf[:], wbuff[off*lba(fs.ssize):(off+1)*lba(fs.ssize)])
					fp.flag &^= faDIRTY
				}
				wcnt = int(cc) * int(fs.ssize)
				continue
			}
			// Fill sector cache with file data.
			if fp.sect != sect && fp.fptr < fp.obj.objsize &&
				fs.disk_read(fp.buf[:], sect, 1) != drOK {
				return bw, fp.abort(frDiskErr)
			}
			fp.sect = sect
		}
		modss := int(fs.modSS(uint32(fp.fptr)))
		wcnt = int(fs.ssize) - modss // Remaining bytes in sector.
		if wcnt > btw {
			wcnt = btw // Clip it by btw.
		}
		copy(fp.buf[modss:], wbuff[:wcnt]) // fit data to the sector.
		fp.flag |= faDIRTY
	}
	fp.flag |= faMODIFIED
	return bw, fr
}

func (fs *FS) f_opendir(dp *dir, path string) (fr fileResult) {
	if dp == nil {
		return frInvalidObject
	} else if fs.fstype == fstypeExFAT {
		return frUnsupported
	}
	path += "\x00" // TODO(soypat): change internal algorithms to non-null terminated strings.

	dp.obj.fs = fs

	fr = dp.follow_path(path)
	if fr != frOK {
		if fr == frNoFile {
			fr = frNoPath
		}
		dp.obj.fs = nil
		return fr
	}

	if dp.fn[nsFLAG]&nsNONAME == 0 {
		if dp.obj.attr&amDIR != 0 {
			// TODO(soypat): implement exFAT here.
			dp.obj.sclust = fs.ld_clust(dp.dir) // Get object allocation info.
		} else {
			fr = frNoPath
		}
	}
	if fr == frOK {
		dp.obj.id = fs.id
		fr = dp.sdi(0) // Rewind directory.
	}
	if fr == frNoFile {
		fr = frNoPath
	}
	if fr != frOK {
		dp.obj.fs = nil
	}
	return fr
}

// f_open opens or creates a file.
func (fsys *FS) f_open(fp *File, name string, mode accessmode) fileResult {
	fsys.trace("f_open", slog.String("name", name), slog.Uint64("mode", uint64(mode)))
	name += "\x00" // TODO(soypat): change internal algorithms to non-null terminated strings.
	if fp == nil {
		return frInvalidObject
	} else if fsys.fstype == fstypeExFAT {
		return frUnsupported
	} else if fsys.perm == 0 {
		return frDenied
	}
	var dj dir
	fp.obj.fs = fsys
	dj.obj.fs = fsys
	res := dj.follow_path(name)
	if res == frOK {
		if dj.fn[nsFLAG]&nsNONAME != 0 {
			res = frInvalidName // Original directory.
		}
	}
	if mode&(faCreateAlways|faOpenAlways|faCreateNew) != 0 {
		// Create a new file branch.
		if res != frOK {
			if res == frNoFile {
				res = dj.register()
			}
			mode |= faCreateAlways
		} else {
			if dj.obj.attr&(amRDO|amDIR) != 0 {
				// Cannot overwrite it (R/O or DIR).
				res = frDenied
			} else if mode&faCreateNew != 0 {
				// Cannot create as new file.
				res = frExist
			}
		}
		if res == frOK && (mode&faCreateAlways) != 0 {
			// Truncate file if overwrite mode.
			// TODO(soypat): implement exFAT here.
			tm := fsys.time()
			binary.LittleEndian.PutUint32(dj.dir[dirCrtTimeOff:], tm)
			binary.LittleEndian.PutUint32(dj.dir[dirModTimeOff:], tm)
			cl := fsys.ld_clust(dj.dir) // Get current cluster chain.
			dj.dir[dirAttrOff] = amARC  // Reset attribute.
			binary.LittleEndian.PutUint32(dj.dir[dirFileSizeOff:], 0)
			fsys.wflag = 1
			if cl != 0 {
				sc := fsys.winsect
				res = fp.obj.remove_chain(cl, 0)
				if res == frOK {
					res = fsys.move_window(sc)
					fsys.last_clst = cl - 1 // Reuse the cluster hole.
				}
			}
		}
	} else {
		// Open an existing file branch.
		if res == frOK {
			if dj.obj.attr&amDIR != 0 {
				// It is a directory.
				res = frNoFile
			} else if mode&faWrite != 0 && dj.obj.attr&amRDO != 0 {
				// R/O violation.
				res = frDenied
			}
		}
	}
	if res != frOK {
		fp.obj.fs = nil
		return res
	}

	if mode&faCreateAlways != 0 {
		mode |= faMODIFIED
	}
	fp.dir_sect = fsys.winsect
	fp.dir_ptr = dj.dir

	// TODO(soypat): implement exFAT here.

	fp.obj.sclust = fsys.ld_clust(dj.dir)
	fp.obj.objsize = int64(binary.LittleEndian.Uint32(dj.dir[dirFileSizeOff:]))

	fp.obj.fs = fsys
	fp.obj.id = fsys.id
	fp.flag = mode
	fp.err = 0
	fp.sect = 0
	fp.fptr = 0
	fp.buf = [512]byte{} // Clear sector buffer.

	if mode&faSEEKEND != 0 && fp.obj.objsize > 0 {
		fp.fptr = fp.obj.objsize
	}

	if mode&faSEEKEND != 0 && fp.obj.objsize > 0 {
		// Seek to end of file. i.e: if Append mode is passed.
		fp.fptr = fp.obj.objsize
		bcs := fsys.csize * fsys.ssize
		clst := fp.obj.sclust
		ofs := fp.obj.objsize
		for ; res == frOK && ofs > int64(bcs); ofs -= int64(bcs) {
			clst = fp.obj.clusterstat(clst)
			if clst <= 1 {
				res = frIntErr
			} else if clst == maxu32 {
				res = frDiskErr
			}
		}
		fp.clust = clst
		if res == frOK && fsys.modSS(uint32(ofs)) != 0 {
			sc := fsys.clst2sect(clst)
			if sc == 0 {
				res = frIntErr
			} else {
				// here we actually perform division and cast to 64bit to avoid underflow.
				fp.sect = sc + lba(ofs/int64(fsys.ssize))
				if fsys.disk_read(fp.buf[:], fp.sect, 1) != drOK {
					res = frDiskErr
				}
			}
		}
	}
	if res != frOK {
		fp.obj.fs = nil
	}
	return res
}

func (dp *dir) f_readdir(fno *FileInfo) fileResult {
	fsys := dp.obj.fs
	fsys.trace("dir:f_readdir")
	if fsys.fstype == fstypeExFAT {
		return frUnsupported
	}
	fr := dp.obj.validate()
	if fr != frOK {
		return fr
	}
	if fno == nil {
		fr = dp.sdi(0)
	} else {
		// Read an item.
		fr = dp.read(false)
		if fr == frNoFile {
			fr = frOK // Ignore end of directory.
		}
		if fr == frOK {
			dp.get_fileinfo(fno)
			fr = dp.next(false) // Increment index for next.
			if fr == frNoFile {
				// Ignore end of directory until next call
				// since we succesfully read fileinfo.
				fr = frOK
			}
		}
	}
	return fr
}

func (dp *dir) read(vol bool) (fr fileResult) {
	fsys := dp.obj.fs
	fsys.trace("dir:read", slog.Bool("vol", vol))
	if fsys.fstype == fstypeExFAT {
		return frUnsupported
	}
	var ord, sum byte
	for dp.sect != 0 {
		fr = fsys.move_window(dp.sect)
		if fr != frOK {
			break
		}
		b := dp.dir[dirNameOff]
		if b == 0 {
			fr = frNoFile
			break
		}
		// TODO: implement exFAT here.
		attr := dp.dir[dirAttrOff] & amMASK
		dp.obj.attr = attr
		if b == mskDDEM || b == '.' || (attr&^amARC == amVOL) != vol {
			// Entry without valid data.
			ord = 0xff
		} else {
			if attr == amLFN {
				if b&mskLLEF != 0 {
					sum = dp.dir[ldirChksumOff]
					b &^= mskLLEF
					ord = b
					dp.blk_ofs = dp.dptr
				}
				// Store LFN validity.
				if b == ord && sum == dp.dir[ldirChksumOff] && fsys.pick_lfn(dp.dir) {
					ord--
				} else {
					ord = 0xff
				}
			} else {
				if ord != 0 || sum != sum_sfn(dp.dir) {
					dp.blk_ofs = maxu32 // No LFN.
				}
				break
			}
		}
		fr = dp.next(false)
		if fr != frOK {
			break
		}
	}
	if fr != frOK {
		dp.sect = 0 // Terminate read op on EOT.
	}
	return fr
}

func (fsys *FS) sync() fileResult {
	fsys.trace("fs:sync")
	fr := fsys.sync_window()
	if fr == frOK && fsys.fstype == fstypeFAT32 && fsys.fsi_flag == 1 {
		// Create FSInfo structure.
		fsys.window_clr()
		binary.LittleEndian.PutUint16(fsys.win[bs55AA:], 0xAA55)
		binary.LittleEndian.PutUint32(fsys.win[fsiLeadSig:], 0x41615252)
		binary.LittleEndian.PutUint32(fsys.win[fsiStrucSig:], 0x61417272)
		binary.LittleEndian.PutUint32(fsys.win[fsiFree_Count:], fsys.free_clst)
		binary.LittleEndian.PutUint32(fsys.win[fsiNxt_Free:], fsys.last_clst)
		fsys.winsect = fsys.volbase + 1
		fsys.disk_write(fsys.win[:], fsys.winsect, 1) // Write backup copy.
		fsys.fsi_flag = 0
	}
	return fr
}

// pick_lfn picks a part of a filename from LFN entry.
func (fsys *FS) pick_lfn(dir []byte) bool {
	fsys.trace("pick_lfn")
	if binary.LittleEndian.Uint16(dir[ldirFstClusLO_Off:]) != 0 {
		return false
	}
	i := 13 * int((dir[ldirOrdOff]&^mskLLEF)-1) // Offset in LFN buffer.
	var wc uint16
	var s int
	for wc = 1; s < 13; s++ {
		uc := binary.LittleEndian.Uint16(dir[lfnOffsets[s]:])
		if wc != 0 {
			if i >= lfnBufSize+1 {
				return false
			}
			fsys.lfnbuf[i] = uc
			wc = uc
			i++
		} else if uc != maxu16 {
			return false
		}
	}
	if dir[ldirOrdOff]&mskLLEF != 0 && wc != 0 {
		// Put terminator if last LFN part and not terminated.
		if i >= lfnBufSize+1 {
			return false
		}
		fsys.lfnbuf[i] = 0
	}
	return true
}

// mount initializes the FS with the given BlockDevice.
func (fsys *FS) mount_volume(bd BlockDevice, ssize uint16, mode uint8) (fr fileResult) {
	_ = str16(fsys.lfnbuf[:0]) // include str16 utility into build for debugging.
	fsys.trace("fs:mount_volume", slog.Int("mode", int(mode)))
	fsys.fstype = fstypeUnknown // Invalidate any previous mount.
	// From here on out we call mount_volume since we don't care about
	// mutexes or file path handling. File path handling is left to
	// the Go standard library which does a much better job than us.
	// See `filepath` and `fs` standard library packages.

	blk, err := makeBlockIndexer(int(ssize))
	if err != nil {
		return frInvalidParameter
	}
	fsys.device = bd
	fsys.id++ // Invalidate open files.
	fsys.blk = blk
	fsys.ssize = ssize
	fsys.perm = Mode(mode)
	fmt := fsys.find_volume(0)

	if fmt == bootsectorstatusDiskError {
		return frDiskErr
	} else if fmt == bootsectorstatusNotFATInvalidBS || fmt == bootsectorstatusNotFATValidBS {
		return frNoFilesystem
	}
	if fsys.dbcTbl == [10]byte{} {
		// TODO(soypat): Codepages, use them.
		fsys.dbcTbl = [10]byte{0x81, 0x9F, 0xE0, 0xFC, 0x40, 0x7E, 0x80, 0xFC}
	}
	if fmt == bootsectorstatusExFAT {
		return fsys.init_exfat()
	}
	return fsys.init_fat()
}

func (fp *File) clmt_clust(ofs int64) (cl uint32) {
	fsys := fp.obj.fs
	fsys.trace("fp:clmt_clust", slog.Int64("ofs", ofs))
	tbl := fp.cltbl[1:] // Top of CLMT.
	cl = uint32(ofs / int64(fsys.ssize) / int64(fsys.csize))
	for {
		ncl := tbl[0]
		if ncl == 0 {
			return 0
		} else if cl < ncl {
			break
		}
		cl -= ncl
		tbl = tbl[1:]
	}
	return cl + tbl[0]
}

func (fsys *FS) init_fat() fileResult { // Part of mount_volume.
	fsys.trace("fs:init_fat")
	baseSector := fsys.winsect
	ss := fsys.ssize
	if fsys.window_u16(bpbBytsPerSec) != uint16(ss) {
		return frInvalidParameter
	}
	// Number of sectors per FAT.
	sectorsPerFAT := uint32(fsys.window_u16(bpbFATSz16))
	if sectorsPerFAT == 0 {
		sectorsPerFAT = fsys.window_u32(bpbFATSz32)
	}
	fsys.fsize = sectorsPerFAT
	fsys.nFATs = fsys.win[bpbNumFATs]
	if fsys.nFATs != 1 && fsys.nFATs != 2 {
		return frNoFilesystem
	}
	sectorsPerFAT *= uint32(fsys.nFATs)
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
	sectorsTotal := uint32(fsys.window_u16(bpbTotSec16))
	if sectorsTotal == 0 {
		sectorsTotal = fsys.window_u32(bpbTotSec32)
	}

	// Number of reserved sectors.
	sectorsReserved := fsys.window_u16(bpbRsvdSecCnt)
	if sectorsReserved == 0 {
		return frNoFilesystem
	}

	// Determine the FAT subtype. RSV+FAT+DIR
	const sizeDirEntry = 32
	sectorsNonApplication := uint32(sectorsReserved) + sectorsPerFAT + uint32(fsys.nrootdir)/(uint32(ss)/sizeDirEntry)
	clustersTotal := (sectorsTotal - sectorsNonApplication) / uint32(fsys.csize)
	if sectorsTotal < sectorsNonApplication || clustersTotal == 0 {
		return frNoFilesystem
	}
	var fmt fstype = fstypeFAT12
	switch {
	case clustersTotal > clustMaxFAT32:
		return frNoFilesystem // Too many clusters for FAT32.
	case clustersTotal > clustMaxFAT16:
		fmt = fstypeFAT32
	case clustersTotal > clustMaxFAT12:
		fmt = fstypeFAT16
	}

	// Boundaries and limits.
	fsys.n_fatent = clustersTotal + 2
	fsys.volbase = baseSector
	fsys.fatbase = baseSector + lba(sectorsReserved)
	fsys.database = baseSector + lba(sectorsNonApplication)
	var neededSizeOfFAT uint32
	if fmt == fstypeFAT32 {
		if fsys.window_u16(bpbFSVer32) != 0 {
			return frNoFilesystem // Unsupported FAT subversion, must be 0.0.
		} else if fsys.nrootdir != 0 {
			return frNoFilesystem // Root directory entry count must be 0.
		}
		fsys.dirbase = lba(fsys.window_u32(bpbRootClus32))
		neededSizeOfFAT = fsys.n_fatent * 4
	} else {
		if fsys.nrootdir == 0 {
			return frNoFilesystem // Root directory entry count must not be 0.
		}
		fsys.dirbase = fsys.fatbase + lba(sectorsPerFAT)
		if fmt == fstypeFAT16 {
			neededSizeOfFAT = fsys.n_fatent * 2
		} else {
			neededSizeOfFAT = fsys.n_fatent*3/2 + fsys.n_fatent&1
		}
	}
	sectorsNeeded := (neededSizeOfFAT + uint32(ss-1)) / uint32(ss)
	if fsys.fsize < sectorsNeeded {
		// TODO(soypat): failing FAT size compare here.
		fsys.logerror("init_fat:sectorsNeeded", slog.Int("sectorsNeeded", int(sectorsNeeded)))
		// return frNoFilesystem // FAT size must not be less than FAT sectors.
	}
	// Initialize cluster allocation information for write ops.
	fsys.last_clst = 0xffff_ffff
	fsys.free_clst = 0xffff_ffff
	fsys.fsi_flag = 1 << 7

	// Update FSInfo.
	if fmt == fstypeFAT32 && fsys.window_u16(bpbFSInfo32) == 1 && fsys.move_window(baseSector+1) == frOK {
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
	fsys.trace("fs:find_volume", slog.Int64("part", part))
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
	fsys.trace("fs:check_fs", slog.Uint64("sect", uint64(sect)))
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
	bsValid := binary.LittleEndian.Uint16(fsys.win[offsetSignature:]) == 0xaa55

	if bsValid && !fsys.window_memcmp(offsetJMPBoot, "\xEB\x76\x90EXFAT   ") {
		return bootsectorstatusExFAT // exFAT VBR.
	}
	b := fsys.win[offsetJMPBoot]
	if b != 0xEB && b != 0xE9 && b != 0xE8 {
		// Not a FAT VBR, BS may be valid/invalid.
		return 3 - b2i[bootsectorstatus](bsValid)
	} else if bsValid && !fsys.window_memcmp(offsetFileSys32, "FAT32   ") {
		return bootsectorstatusFAT // FAT32 VBR.
	}
	// TODO(soypat): Support early MS-DOS FAT.
	return bootsectorstatusNotFATInvalidBS
}

func (obj *objid) clusterstat(clst uint32) (val uint32) {
	fsys := obj.fs
	fsys.trace("fs:clusterstat", slog.Uint64("clst", uint64(clst)))
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

// put_clusterstat changes the value of a FAT entry.
func (fsys *FS) put_clusterstat(cluster, value uint32) fileResult {
	fsys.trace("fs:put_clusterstat", slog.Uint64("cluster", uint64(cluster)), slog.Uint64("value", uint64(value)))
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
	fsys.trace("fs:move_window", slog.Uint64("sector", uint64(sector)))
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
	areEqual := off+uint16(len(data)) <= uint16(len(fsys.win)) && unsafe.String((*byte)(unsafe.Pointer(&fsys.win[off])), len(data)) == data
	return !areEqual
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

// lfnlen returns the LFN length.
func (fsys *FS) lfnlen() (ln int) {
	for ; ln < len(fsys.lfnbuf) && fsys.lfnbuf[ln] != 0; ln++ {
	}
	return ln
}

func (fsys *FS) sync_window() (fr fileResult) {
	fsys.trace("fs:sync_window")
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

// dir_clear fills a cluster with zeros on the disk.
func (fsys *FS) dir_clear(clst uint32) fileResult {
	fsys.trace("fs:dir_clear", slog.Uint64("clst", uint64(clst)))
	if fsys.sync_window() != frOK {
		return frDiskErr
	}
	// Set window to top of the cluster.
	sect := fsys.clst2sect(clst)
	fsys.winsect = sect
	fsys.window_clr()
	result := fsys.disk_erase(sect, int(fsys.csize))
	if result != drOK {
		fsys.logerror("dir_clear:dc", slog.Int("dret", int(result)))
		return frDiskErr
	}
	return frOK
}

func (fsys *FS) time() uint32 {
	return 0
}

// ld_clust loads start(top) cluster value of the SFN entry using the key entry buffer.
func (fsys *FS) ld_clust(bdir []byte) (cl uint32) {
	cl = uint32(binary.LittleEndian.Uint16(bdir[dirFstClusLOOff:]))
	if fsys.fstype == fstypeFAT32 {
		cl |= uint32(binary.LittleEndian.Uint16(bdir[dirFstClusHIOff:])) << 16
	}
	return cl
}

// st_clust stores a cluster value to the SFN entry using the key entry buffer.
func (fsys *FS) st_clust(dir []byte, cl uint32) {
	binary.LittleEndian.PutUint16(dir[dirFstClusLOOff:], uint16(cl))
	if fsys.fstype == fstypeFAT32 {
		binary.LittleEndian.PutUint16(dir[dirFstClusHIOff:], uint16(cl>>16))
	}
}

func (fsys *FS) disk_write(buf []byte, sector lba, numsectors int) diskresult {
	if fsys.perm&ModeWrite == 0 {
		return drWriteProtected
	}
	fsys.trace("fs:disk_write", slog.Uint64("start", uint64(sector)), slog.Int("numsectors", numsectors))
	if fsys.blk.off(int64(len(buf))) != 0 || fsys.blk._divideBlockSize(int64(len(buf))) != int64(numsectors) {
		fsys.logerror("disk_write:unaligned")
		return drParError
	}
	_, err := fsys.device.WriteBlocks(buf, int64(sector))
	if err != nil {
		fsys.logerror("disk_write", slog.String("err", err.Error()))
		return drError
	}
	return drOK
}
func (fsys *FS) disk_read(dst []byte, sector lba, numsectors int) diskresult {
	fsys.trace("fs:disk_read", slog.Uint64("start", uint64(sector)), slog.Int("numsectors", numsectors))
	off := fsys.blk.off(int64(len(dst)))
	blocks := fsys.blk._divideBlockSize(int64(len(dst)))
	if off != 0 || blocks != int64(numsectors) {
		fsys.logerror("disk_read:unaligned")
		return drParError
	}
	_, err := fsys.device.ReadBlocks(dst, int64(sector))
	if err != nil {
		fsys.logerror("disk_read", slog.String("err", err.Error()))
		return drError
	}
	return drOK
}
func (fsys *FS) disk_erase(startSector lba, numSectors int) diskresult {
	fsys.trace("fs:disk_erase", slog.Uint64("start", uint64(startSector)), slog.Int("numsectors", numSectors))
	err := fsys.device.EraseBlocks(int64(startSector), int64(numSectors))
	if err != nil {
		fsys.logerror("disk_erase", slog.String("err", err.Error()))
		return drError
	}
	return drOK
}

func (fsys *FS) dbc_1st(c byte) bool {
	// fsys.trace("fs:dcb_1st")
	if c >= fsys.dbcTbl[0] {
		return c <= fsys.dbcTbl[1] || (c >= fsys.dbcTbl[2] && c <= fsys.dbcTbl[3])
	}
	return false
	// TODO(soypat): Revise code page effect here.
	// return (fsys.ffCodePage == 0 || fsys.ffCodePage >= 900) &&
	// 	(c >= fsys.dbcTbl[0] || (c >= fsys.dbcTbl[2] && c <= fsys.dbcTbl[3]))
}

func (fsys *FS) dbc_2nd(c byte) bool {
	// fsys.trace("fs:dcb_2nd")
	dbc := &fsys.dbcTbl
	if c >= dbc[4] {
		return c <= dbc[5] || (c >= dbc[6] && c <= dbc[7]) ||
			(c >= dbc[8] && c <= dbc[9])
	}
	return false
	// TODO(soypat): Revise code page effect here.
	// return (fsys.ffCodePage == 0 || fsys.ffCodePage >= 900) &&
	// 	(c >= fsys.dbcTbl[4] || (c >= fsys.dbcTbl[6] && c <= fsys.dbcTbl[7]) ||
	// 		(c >= fsys.dbcTbl[8] && c <= fsys.dbcTbl[9]))
}

func (fsys *FS) window_clr() {
	fsys.win = [len(fsys.win)]byte{} // Effectively a memclear.
}

var lfnOffsets = [...]byte{1, 3, 5, 7, 9, 14, 16, 18, 20, 22, 24, 28, 30}

func (fsys *FS) put_lfn(dir []byte, ord, sum byte) {
	fsys.trace("put_lfn", slog.Uint64("ord", uint64(ord)))
	// TODO(soypat): maybe this should receive a *dir and avoid two word copies?
	lfn := &fsys.lfnbuf
	dir[ldirChksumOff] = sum
	dir[ldirAttrOff] = amLFN
	dir[ldirTypeOff] = 0
	binary.LittleEndian.PutUint16(dir[ldirOrdOff:], 0)
	i := uint32(ord-1) * 13
	var wc uint16
	var s uint32
	for s < 13 {
		if wc != maxu16 {
			wc = lfn[i]
			i++
		}
		off := lfnOffsets[s]
		binary.LittleEndian.PutUint16(dir[off:], wc)
		if wc == 0 {
			wc = maxu16
		}
		s++
	}
	if wc == maxu16 || lfn[i] == 0 {
		ord |= mskLLEF
	}
	dir[ldirOrdOff] = ord
}

func (fs *FS) change_bitmap(clst, ncl uint32, bv bool) fileResult {
	fs.trace("fs:change_bitmap", slog.Uint64("clst", uint64(clst)), slog.Uint64("ncl", uint64(ncl)), slog.Bool("bv", bv))
	clst -= 2 // First bit corresponds to cluster #2.
	clstDiv8 := clst / 8
	sect := fs.bitbase + lba(fs.divSS(clstDiv8))
	i := fs.modSS(clstDiv8)
	var mask byte = 1 << (clst % 8)
	for {
		if fs.move_window(sect) != frOK {
			return frDiskErr
		}
		sect++
		for {
			for {
				if bv == (fs.win[i]&mask != 0) {
					// Unexpected bit value.
					return frIntErr
				}
				fs.win[i] ^= mask
				fs.wflag = 1
				ncl--
				mask <<= 1
				if ncl == 0 {
					return frOK
				} else if mask == 0 {
					break
				}
			}
			mask = 1
			i++
			if i == uint32(fs.ssize) {
				break
			}
		}
		i = 0
		runtime.Gosched()
	}
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

const slogLevelTrace = slog.LevelDebug - 2

func (fsys *FS) logattrs(level slog.Level, msg string, attrs ...slog.Attr) {
	if fsys.log != nil {
		fsys.log.LogAttrs(context.Background(), level, msg, attrs...)
	}
}

func (fsys *FS) trace(msg string, attrs ...slog.Attr) {
	fsys.logattrs(slogLevelTrace, msg, attrs...)
}
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

// register registers the object to the directory. Returns:
//   - frOK: successful
//   - frDenied:no free entry or too many SFN collisions
//   - frDiskErr: disk error
func (dp *dir) register() (fr fileResult) {
	const maxCollisions = 100
	fsys := dp.obj.fs
	fsys.trace("dir:register")
	if dp.fn[nsFLAG]&(nsDOT|nsNONAME) != 0 {
		return frInvalidName
	}
	ln := fsys.lfnlen()
	if fsys.fstype == fstypeExFAT {
		return frUnsupported // Not implemented.
	}
	var sn [12]byte
	copy(sn[:], dp.fn[:])
	if sn[nsFLAG]&nsLOSS != 0 {
		// LFN is out of 8.3 format; generate numbered name.
		dp.fn[nsFLAG] = nsNOLFN
		n := uint32(1)
		for ; n < maxCollisions; n++ {
			fsys.gen_numname(dp.fn[:], sn[:], fsys.lfnbuf[:], n)
			fr = dp.find()
			if fr != frOK {
				break
			}
		}
		if n == maxCollisions {
			return frDenied // Abort: too many collisions.
		} else if fr != frNoFile {
			return fr // Probably disk error, we want NoFile result that indicates no collision.
		}
		dp.fn[nsFLAG] = sn[nsFLAG]
	}

	// Create an SFN with/without LFNs
	nent := 1
	if sn[nsFLAG]&nsLFN != 0 {
		nent = (ln+12)/13 + 1
	}
	fr = dp.alloc(nent)
	nent--
	if fr == frOK && nent != 0 {
		// Set LFN entry if needed.
		fr = dp.sdi(dp.dptr - uint32(nent*sizeDirEntry))
		if fr == frOK {
			sum := sum_sfn(dp.fn[:])
			for {
				// Store LFN entries in bottom first.
				fr = fsys.move_window(dp.sect)
				if fr != frOK {
					break
				}
				fsys.put_lfn(dp.dir[:], byte(nent), sum)
				fsys.wflag = 1
				const stretchTable = false
				fr = dp.next(stretchTable)
				nent--
				if fr != frOK || nent <= 0 {
					break
				}
			}
		}
	}
	if fr == frOK {
		fr = fsys.move_window(dp.sect)
		if fr == frOK {
			dp.dirbuf_clr()
			copy(dp.dir[dirNameOff:], dp.fn[:11])
			dp.dir[dirNTresOff] = dp.fn[nsFLAG] & (nsBODY | nsEXT)
			fsys.wflag = 1
		}
	}
	return fr

}

// alloc reserves a block of directory entries. Returns:
//   - frOK: successful
//   - frDenied: could not stretch the directory table or no free entry
//   - frDiskErr: disk error
func (dp *dir) alloc(nent int) (fr fileResult) {
	fsys := dp.obj.fs
	fsys.trace("dir:alloc")
	fr = dp.sdi(0)
	n := 0
	for fr == frOK {
		fr = fsys.move_window(dp.sect)
		if fr != frOK {
			break
		}
		isEx := fsys.fstype == fstypeExFAT
		dname := dp.dir[dirNameOff]
		if (isEx && dp.dir[xdirType]&0x80 == 0) || (!isEx && (dname == mskDDEM || dname == 0)) {
			// Entry is free.
			n++
			if n == nent {
				break // Block of contiguous free entries is found.
			}
		} else {
			n = 0
		}
		const enableStretching = true
		fr = dp.next(enableStretching)
	}

	if fr == frNoFile {
		fr = frDenied
	}
	return fr
}

// follow_path traverses the directory tree
func (dp *dir) follow_path(path string) (fr fileResult) {
	fsys := dp.obj.fs
	fsys.trace("dir:follow_path", slog.String("path", path))
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
		path, fr = dp.create_name(path)
		if fr != frOK {
			break
		}
		fr = dp.find()
		ns := dp.fn[nsFLAG]
		if fr != frOK {
			if fr == frNoFile && ns&nsLAST == 0 {
				// Could not find the object.
				fr = frNoPath
			}
			break
		}
		if ns&nsLAST != 0 {
			// Last segment match. Function completed.
			break
		}
		// Get into the sub directory.
		if dp.obj.attr&amDIR == 0 {
			// Cannot follow because it is a file.
			fr = frNoPath
			break
		}
		// Open next directory:
		if fsys.fstype == fstypeExFAT {
			// Save containing directory for next dir.
			dp.obj.c_scl = dp.obj.sclust
			dp.obj.c_size = uint32(dp.obj.objsize&0xFFFFFF00) | uint32(dp.obj.stat)
			dp.obj.c_ofs = dp.blk_ofs
			dp.obj.init_alloc_info()
		} else {
			off := fsys.modSS(dp.dptr)
			dp.obj.sclust = fsys.ld_clust(fsys.win[off:])
		}
	}
	return fr
}

func (dp *dir) dirbuf_clr() {
	for i := 0; i < sizeDirEntry; i++ {
		dp.dir[i] = 0
	}
}

// find searches the directory for an object matching the given name. Returns:
//   - frOK: successful
//   - frNoFile: no file found
//   - frDiskErr: disk error
func (dp *dir) find() fileResult {
	fsys := dp.obj.fs
	fsys.trace("dir:find")
	fr := dp.sdi(0) // Rewind directory object.
	if fr != frOK {
		return fr
	}
	if fsys.fstype == fstypeExFAT {
		return frUnsupported // TODO(soypat): implement exFAT.
	}
	var ord, sum byte = 0xff, 0xff
	dp.blk_ofs = 0xffff_ffff // Reset LFN sequence.
	for fr == frOK {
		fr = fsys.move_window(dp.sect)
		if fr != frOK {
			break
		}
		c := dp.dir[dirNameOff]
		if c == 0 {
			// Reached to end of table.
			fr = frNoFile
			break
		}
		// LFN LOGIC START.
		attr := dp.dir[dirAttrOff] & amMASK
		dp.obj.attr = attr
		if c == mskDDEM || (attr&amVOL != 0 && attr != amLFN) {
			ord = 0xff
			dp.blk_ofs = 0xffff_ffff // Reset LFN sequence.
		} else {
			if attr == amLFN {
				if dp.fn[nsFLAG]&nsNOLFN == 0 {
					if c&mskLLEF != 0 {
						// Start of LFN sequence.
						sum = dp.dir[ldirChksumOff]
						c &^= mskLLEF
						ord = c
						dp.blk_ofs = dp.dptr
					}
					if c == ord && sum == dp.dir[ldirChksumOff] && fsys.cmp_lfn(dp.dir[:]) {
						ord--
					} else {
						ord = 0xff
					}
				}
			} else {
				if ord == 0 && sum == sum_sfn(dp.dir[:]) {
					break // LFN matches.
				} else if dp.fn[nsFLAG]&nsLOSS == 0 && !memcmp(&dp.dir[0], &dp.fn[0], 11) {
					break // SFN matches.
				}
				// Reset LFN sequence.
				ord = 0xff
				dp.blk_ofs = 0xffff_ffff
			}
		}
		const stretchTable = false
		fr = dp.next(stretchTable)
	}
	return fr
}

// next advances the directory table to the next entry. Returns:
//   - frOK: successful
//   - frNoFile: end of table
//   - frDenied: could not stretch the directory table
func (dp *dir) next(stretch bool) fileResult {
	fsys := dp.obj.fs
	fsys.trace("dir:next", slog.Bool("stretch", stretch))
	ofs := dp.dptr + sizeDirEntry
	if ofs >= maxDIREx || (ofs >= maxDIR && fsys.fstype != fstypeExFAT) {
		dp.sect = 0 // Disable if reached maximum offset.
	}
	if dp.sect == 0 {
		return frNoFile
	}
	modOfs := fsys.modSS(ofs)
	if modOfs != 0 {
		// Sector unchanged.
		goto AllOK
	}
	// Sector changed.
	dp.sect++
	if dp.clust == 0 {
		// Static table.
		if ofs/sizeDirEntry >= uint32(fsys.nrootdir) {
			dp.sect = 0
			return frNoFile // EOT: Reached end of static table.
		}
		goto AllOK
	}

	// Create a anonymous scope so goto does not jump over variable declarations.
	{
		// Dynamic table.
		divOfs := fsys.divSS(ofs)
		if divOfs&uint32(fsys.csize-1) != 0 {
			goto AllOK // Cluster not changed.
		}
		clst := dp.obj.clusterstat(dp.clust)
		if clst <= 1 {
			return frIntErr
		} else if clst == 0xffff_ffff {
			return frDiskErr
		} else if clst >= fsys.n_fatent {
			if !stretch {
				// If no stretch report EOT.
				dp.sect = 0
				return frNoFile
			}
			// Allocate dat clusta.
			clst = dp.obj.create_chain(0)
			switch clst {
			case 0:
				return frDenied
			case 1:
				return frIntErr
			case maxu32:
				return frDiskErr
			}
			// Try cleaning the stretched table.
			if fsys.dir_clear(clst) != frOK {
				return frDiskErr
			}
			dp.obj.stat |= 4 // ExFAT, mark directory as stretched.
		}
		dp.clust = clst // Next cluster.
		dp.sect = fsys.clst2sect(clst)
	}

AllOK:
	dp.dptr = ofs                   // Current entry.
	dp.dir = dp.obj.fs.win[modOfs:] // Pointer to the entry in win[].
	return frOK
}

func (dp *dir) create_name(path string) (string, fileResult) {
	var (
		p    = path
		fsys = dp.obj.fs
		lfn  = fsys.lfnbuf[:]
		di   = 0
	)
	fsys.trace("dir:create_name")
	var wc uint16
	for {
		uc, plen := utf8.DecodeRuneInString(p)
		if uc == utf8.RuneError {
			return "", frInvalidName
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
			return "", frInvalidName
		}
		if di >= lfnBufSize {
			return "", frInvalidName
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
	path = p // Returns next segment.

	for di > 0 {
		wc = lfn[di-1]
		if wc != ' ' && wc != '.' {
			break
		}
		di--
	}
	lfn[di] = 0
	if di == 0 {
		return path, frInvalidName // Reject null name.
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
	codepageEnabled := len(fsys.codepage) != 0
	for si < len(lfn) {
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

		if wc >= 0x80 && codepageEnabled {
			// Possible extended character.
			cf |= nsLFN // Flag LFN entry needs creation.
			wc = ff_uni2oem(rune(wc), fsys.codepage)
			if wc&0x80 != 0 {
				wc = uint16(fsys.exCvt[wc&0x7f]) // Convert extended character to upper (SBCS).
			}
			wc = ff_uni2oem(rune(wc), fsys.codepage)
			if wc&0x80 != 0 {
				wc = uint16(fsys.exCvt[wc&0x7f]) // Convert extended character to upper (SBCS).
			}
		}
		if wc >= 0x100 {
			// This is a DBC.
			if i >= ni-1 {
				// Possible field overflow.
				cf |= nsLOSS | nsLFN
				i = ni
				continue
			}
			dp.fn[i] = byte(wc >> 8)
			i++
		} else {
			if wc == 0 || strings.IndexByte("+,;=[]", byte(wc)) >= 0 {
				wc = '_'             // Replace illegal characters for SFN.
				cf |= nsLOSS | nsLFN // Flag the lossy conversion.
			} else {
				b |= b2u8(isUpper(wc)) << 1
				if isLower(wc) {
					b |= 1
					wc -= 0x20
				}
			}
		}
		dp.fn[i] = byte(wc)
		i++
	}
	if dp.fn[0] == mskDDEM {
		// If the first character collides with DDEM, replace it with RDDEM.
		dp.fn[0] = mskRDDEM
	}
	if ni == 8 {
		// Shift capital flags if no extension.
		b <<= 2
	}
	if b&0x0c == 0x0c || b&0x03 == 0x03 {
		//  LFN entry needs to be created if composite capitals.
		cf |= nsLFN
	}
	if cf&nsLFN == 0 {
		if b&1 != 0 {
			cf |= nsEXT
		}
		if b&4 != 0 {
			cf |= nsBODY
		}
	}
	dp.fn[nsFLAG] = cf // SFN is created into dp->fn[]
	return path, frOK
}

func (dp *dir) sdi(ofs uint32) fileResult {
	fsys := dp.obj.fs
	fsys.trace("dir:sdi", slog.Uint64("ofs", uint64(ofs)))
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
	dp.dir = fsys.win[fsys.modSS(ofs):]
	return frOK
}

func (dp *dir) get_fileinfo(fno *FileInfo) {
	fsys := dp.obj.fs

	fno.fname[0] = 0 // Invalidate.
	if dp.sect == 0 {
		return // End of directory reached.
	} else if fsys.fstype == fstypeExFAT {
		return
	}
	// TODO(soypat): implement exFAT here.
	var si, di int
	var wc uint16
	if dp.blk_ofs != maxu32 {
		// Get LFN if available.
		var hs uint16
		for fsys.lfnbuf[si] != 0 {
			wc = fsys.lfnbuf[si]
			si++
			if hs == 0 && isSurrogate(wc) {
				hs = wc // Low surrogate.
				continue
			}
			nw := put_utf8(rune(hs)<<16|rune(wc), fno.fname[di:])
			if nw == 0 {
				// Buffer overflow or wrong char.
				di = 0
				break
			}
			di += nw
			hs = 0
		}
		if hs != 0 {
			di = 0 // Broken surrogate pair.
		}
		fno.fname[di] = 0 // Terminate the LFN.
	}

	si, di = 0, 0
	for si < 11 {
		// Get SFN.
		wc = uint16(dp.dir[si])
		si++
		if wc == ' ' {
			continue
		} else if wc == mskRDDEM {
			wc = mskDDEM
		}
		if si == 9 && di < sfnBufSize {
			fno.altname[di] = '.'
			di++
		}
		b1 := fsys.dbc_1st(byte(wc))
		b2 := fsys.dbc_2nd(dp.dir[si])
		if b1 && si != 8 && si != 11 && b2 {
			wc = wc<<8 | uint16(dp.dir[si])
			si++
		}
		wc = ff_oem2uni(wc, fsys.codepage)

		if wc == 0 {
			di = 0 // Wrong char.
			break
		}

		nw := put_utf8(rune(wc), fno.altname[di:sfnBufSize])
		if nw == 0 {
			di = 0
			break
		}
		di += nw
	}
	if fno.fname[0] == 0 {
		// LFN is invalid: altname needs to be copied to fname.
		if di == 0 {
			fno.fname[di] = '?'
			di++
		} else {
			si, di = 0, 0
			lcf := byte(nsBODY)
			for fno.altname[si] != 0 {
				wc = uint16(fno.altname[si])
				if wc == '.' {
					lcf = nsEXT
				}
				if isUpper(wc) && dp.dir[dirNTresOff]&lcf != 0 {
					wc += 0x20
				}
				fno.fname[di] = byte(wc)
				si++
				di++
			}
		}
		fno.fname[di] = 0 // Terminate the LFN.
		if dp.dir[dirNTresOff] == 0 {
			// Altname not needed nor case info exists.
			fno.altname[0] = 0
		}
	}
	fno.fattrib = dp.dir[dirAttrOff] & amMASK
	fno.fsize = int64(binary.LittleEndian.Uint32(dp.dir[dirFileSizeOff:]))
	fno.ftime = binary.LittleEndian.Uint16(dp.dir[dirModTimeOff:])
	fno.fdate = binary.LittleEndian.Uint16(dp.dir[dirModTimeOff+2:])
}

func put_utf8(r rune, buf []byte) int {
	if utf8.RuneLen(r) > len(buf) {
		return 0
	}
	return int(utf8.EncodeRune(buf, r))
}

// create_chain stretches or creates a chain. Returns:
//   - 0: No free cluster.
//   - 1: Internal error.
//   - 0xffff_ffff: Disk error.
//   - >= 2: New cluster number.
func (obj *objid) create_chain(clst uint32) uint32 {
	fsys := obj.fs
	fsys.trace("obj:create_chain", slog.Uint64("clst", uint64(clst)))
	var cs, ncl, scl uint32
	if clst == 0 {
		// Request to create a new chain.
		scl = fsys.last_clst
		if scl == 0 || scl >= fsys.n_fatent {
			scl = 1
		}
	} else {
		// Stretch a chain.
		cs = obj.clusterstat(clst)
		if cs < 2 {
			fsys.logerror("create_chain:insanity")
			return 1
		} else if cs == maxu32 || cs < fsys.n_fatent {
			// Disk error or it is already followed by next cluster.
			return cs
		}
		// Suggested cluster to start to find free cluster.
		scl = clst
	}
	if fsys.free_clst == 0 {
		return 0 // No free cluster.
	} else if fsys.fstype == fstypeExFAT {
		return 1 // TODO(soypat): implement exFAT.
	}
	if scl == clst {
		ncl = scl + 1
		if ncl >= fsys.n_fatent {
			ncl = 2
		}
		cs = obj.clusterstat(ncl)
		if cs == 1 || cs == maxu32 {
			return cs // Return error as is.
		}
		if cs != 0 {
			// Is not free. Start at suggested cluster.
			cs = fsys.last_clst
			if cs >= 2 && cs < fsys.n_fatent {
				scl = cs
			}
			ncl = 0
		}
	}
	if ncl == 0 {
		// New cluster not contiguous, find another fragment.
		ncl = scl
		for {
			ncl++
			if ncl >= fsys.n_fatent {
				ncl = 2
				if ncl > scl {
					return 0 // No free cluster.
				}
			}
			cs = obj.clusterstat(ncl)
			if cs == 0 {
				break
			} else if cs == 1 || cs == maxu32 {
				return cs // Return error as is.
			} else if ncl == scl {
				return 0 // No free cluster.
			}
		}
	}
	// Make new cluster EOC.
	fr := fsys.put_clusterstat(ncl, maxu32)
	if fr == frOK && clst != 0 {
		// Link cluster to previous one if needed.
		fr = fsys.put_clusterstat(clst, ncl)
	}
	if fr == frOK {
		fsys.last_clst = ncl
		if fsys.free_clst <= fsys.n_fatent-2 {
			fsys.free_clst--
		}
		fsys.fsi_flag |= 1
	} else {
		if fr == frDiskErr {
			ncl = maxu32
		} else {
			ncl = 1
		}
	}
	return ncl
}

// remove_chain removes a chain from cluster clst. pclst is the previous cluster of clst.
// pclst is zero if the entire chain is to be removed. Returns:
//   - frOK: successful
//   - frDiskErr: disk error
func (obj *objid) remove_chain(clst, pclst uint32) (res fileResult) {
	fsys := obj.fs
	fsys.trace("obj:remove_chain", slog.Uint64("clst", uint64(clst)), slog.Uint64("pclst", uint64(pclst)))
	if clst < 2 || clst >= fsys.n_fatent {
		return frIntErr // Invalid range.
	}

	if pclst != 0 && (fsys.fstype != fstypeExFAT || obj.stat != 2) {
		// Mark previous cluster EOC on the FAT if exists.
		res = fsys.put_clusterstat(pclst, maxu32)
		if res != frOK {
			return res
		}
	}

	// Remove the chain.
	ecl := clst
	scl := clst
removeloop:
	for clst < fsys.n_fatent {
		nxt := obj.clusterstat(clst)
		switch nxt {
		case 0:
			break removeloop
		case 1:
			return frIntErr
		case maxu32:
			return frDiskErr
		}
		if fsys.fstype != fstypeExFAT {
			// Make cluster 'free' on the FAT.
			res = fsys.put_clusterstat(clst, 0)
			if res != frOK {
				return res
			}
			if fsys.free_clst < fsys.n_fatent-2 {
				fsys.free_clst++
				fsys.fsi_flag |= 1
			}
			if ecl+1 == nxt {
				ecl = nxt
			} else {
				if fsys.fstype == fstypeExFAT {
					// Free the block.
					res = fsys.change_bitmap(scl, ecl-scl+1, false)
					if res != frOK {
						return res
					}
				}
				scl = nxt
				ecl = nxt
			}
			clst = nxt
		}
	}

	// TODO(soypat): implement exFAT here.

	return frOK
}

func (obj *objid) init_alloc_info() {
	fsys := obj.fs
	obj.sclust = binary.LittleEndian.Uint32(fsys.dirbuf[xdirFstClus:])
	obj.objsize = int64(binary.LittleEndian.Uint64(fsys.dirbuf[xdirFileSize:]))
	obj.stat = fsys.dirbuf[xdirGenFlags] & 2
	obj.n_frag = 0
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

func b2u8(b bool) uint8 {
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

func isUpper[T _integer](c T) bool   { return 'A' <= c && c <= 'Z' }
func isLower[T _integer](c T) bool   { return 'a' <= c && c <= 'z' }
func isDigit[T _integer](c T) bool   { return '0' <= c && c <= '9' }
func isSep[T _integer](c T) bool     { return c == '/' || c == '\\' }
func isTermLFN[T _integer](c T) bool { return c < ' ' }
func isSurrogate(c uint16) bool      { return c >= 0xd800 && c <= 0xdfff }
func isSurrogateH(c uint16) bool     { return c >= 0xd800 && c <= 0xdbff }
func isSurrogateL(c uint16) bool     { return c >= 0xdc00 && c <= 0xdfff }

// func isTermNoLFN(c byte) bool { return c < '!' }

func sum_sfn(sfn []byte) (sum byte) {
	for i := byte(0); i < 11; i++ {
		sum = (sum >> 1) + (sum << 7) + sfn[i]
	}
	return sum
}

func memcmp(a, b *byte, n int) bool {
	return unsafe.String(a, n) != unsafe.String(b, n)
}

// cmp_lfn returns true if entry matches LFN.
func (fsys *FS) cmp_lfn(dir []byte) bool {
	fsys.trace("fs:cmp_lfn")
	lfn := fsys.lfnbuf[:]
	if binary.LittleEndian.Uint16(dir[ldirFstClusLO_Off:]) != 0 {
		return false
	}
	i := int(dir[ldirOrdOff]&0x3F-1) * 13 // Offset in the LFN buffer.

	var wc uint16 = 1
	for s := 0; s < 13; s++ {
		uc := binary.LittleEndian.Uint16(dir[lfnOffsets[s]:])
		if wc != 0 {
			// TODO: optimize branching below after validated.
			lfnc := rune(lfn[i])
			w1 := ff_wtoupper(rune(uc))
			w2 := ff_wtoupper(lfnc)
			if i >= lfnBufSize+1 || w1 != w2 {
				return false
			}
			i++
			wc = uc
		} else {
			if uc != 0xFFFF {
				return false
			}
		}
	}
	return !(dir[ldirOrdOff]&mskLLEF != 0 && wc != 0 && lfn[i] != 0)
	// TODO(soypat): check if below is equivalent:
	// return dir[ldirOrdOff]&mskLLEF == 0 || wc == 0 || lfn[i] == 0
}

func (fsys *FS) gen_numname(dst, src []byte, lfn []uint16, seq uint32) {
	fsys.trace("fs:gen_numname")
	copy(dst[:11], src) // Prepare SFN to be modified.
	if seq > 5 {
		// On many collisions, generate a hash number instead of sequential number.
		sreg := seq
		for lfn[0] != 0 {
			// Create CRC as hash value.
			wc := lfn[0]
			for i := 0; i < 16; i++ {
				sreg = (sreg << 1) + uint32(wc&1)
				wc >>= 1
				if sreg&0x10000 != 0 {
					sreg ^= 0x11021
				}
			}
		}
		seq = sreg
	}

	// Make suffix with hexadecimal.
	var ns [8]byte
	i := 7
	for {
		c := byte((seq % 16) + '0')
		seq /= 16
		if c > '9' {
			c += 7
		}
		ns[i] = c
		i--
		if i == 0 || seq == 0 {
			break
		}
	}
	ns[i] = '~'

	// Append suffix to SFN body.
	j := 0
	for ; j < i && dst[j] != ' '; j++ {
		if fsys.dbc_1st(dst[j]) {
			if j == i-1 {
				break
			}
			j++
		}
	}

	// Append suffix.
	for {
		if i < 8 {
			dst[j] = ns[i]
			i++
		} else {
			dst[j] = ' '
		}
		j++
		if j >= 8 {
			break
		}
	}
}

func bstr(s []byte) []byte {
	i := 0
	for i < len(s) && s[i] != 0 {
		i++
	}
	return s[:i]
}

func str(s []byte) string {
	var buf []byte
	return string(append(buf, bstr(s)...))
}

func str16(s []uint16) string {
	if len(s) == 0 {
		return ""
	}
	var buf []byte
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == 0 || b >= utf8.RuneError {
			return string(buf)
		} else if b >= 0x80 {
			buf = append(buf, byte(b>>8))
		}
		buf = append(buf, byte(b))
	}
	return string(buf)
}
