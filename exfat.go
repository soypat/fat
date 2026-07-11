//go:build !fat_noexfat && !fat_nolfn

package fat

import (
	"encoding/binary"
	"log/slog"
	"runtime"
	"unicode/utf16"
)

// exfatEnabled reports whether this build has exFAT support (FatFs'
// FF_FS_EXFAT). Compile with the fat_noexfat build tag to disable exFAT
// and reduce binary size; fat_nolfn also disables exFAT since exFAT
// requires long file name support.
const exfatEnabled = true

// dirbuffer is the exFAT directory entry set scratchpad, sized to hold the
// longest possible entry set: (255+44)/15*32 = 608 bytes. Zero-sized when
// exFAT support is compiled out.
type dirbuffer = [608]byte

// maxdirb returns the size in bytes of an exFAT entry set holding a name of
// nc characters: 85 + C0 + as many C1 entries as needed (15 chars each).
func maxdirb(nc uint32) uint32 { return (nc + 44) / 15 * 32 }

// init_exfat is part of mount_volume: the window holds the exFAT VBR.
func (fsys *FS) init_exfat() fileResult {
	fsys.trace("fs:init_exfat")
	bsect := fsys.winsect
	ss := uint32(fsys.ssize)
	for i := bpbZeroedEx; i < bpbZeroedEx+53; i++ {
		if fsys.win[i] != 0 {
			return frNoFilesystem // Check zero filler.
		}
	}
	if fsys.window_u16(bpbFSVerEx) != 0x100 {
		return frNoFilesystem // Check exFAT version (must be version 1.0).
	}
	if 1<<fsys.win[bpbBytsPerSecEx] != ss {
		// BPB_BytsPerSecEx must be equal to the physical sector size. Only
		// 512-byte sectors are supported since win[] is fixed size.
		return frNoFilesystem
	}
	maxlba := binary.LittleEndian.Uint64(fsys.win[bpbTotSecEx:]) + uint64(bsect) // Last LBA of the volume + 1.
	if maxlba >= 0x1_0000_0000 {
		return frNoFilesystem // Cannot be accessed in 32-bit LBA.
	}
	fsys.fsize = fsys.window_u32(bpbFatSzEx) // Number of sectors per FAT.
	fsys.nFATs = fsys.win[bpbNumFATsEx]
	if fsys.nFATs != 1 {
		return frNoFilesystem // Supports only 1 FAT.
	}
	fsys.csize = 1 << fsys.win[bpbSecPerClusEx] // Cluster size in sectors.
	if fsys.csize == 0 {
		return frNoFilesystem // Must be 1..32768 sectors.
	}
	ncl := fsys.window_u32(bpbNumClusEx) // Number of clusters.
	if ncl > clustMaxExFAT {
		return frNoFilesystem // Too many clusters.
	}
	fsys.n_fatent = ncl + 2

	// Boundaries and limits.
	fsys.volbase = bsect
	fsys.database = bsect + lba(fsys.window_u32(bpbDataOfsEx))
	fsys.fatbase = bsect + lba(fsys.window_u32(bpbFatOfsEx))
	if maxlba < uint64(fsys.database)+uint64(ncl)*uint64(fsys.csize) {
		return frNoFilesystem // Volume size must not be smaller than required.
	}
	fsys.dirbase = lba(fsys.window_u32(bpbRootClusEx)) // Root directory start cluster.

	// Get bitmap location and check that it is contiguous (implementation assumption).
	var so, i uint32
	for {
		if i == 0 {
			if so >= uint32(fsys.csize) {
				return frNoFilesystem // Bitmap entry not found in first cluster of root dir.
			}
			if fsys.move_window(fsys.clst2sect(uint32(fsys.dirbase))+lba(so)) != frOK {
				return frDiskErr
			}
			so++
		}
		if fsys.win[i] == etBITMAP {
			break
		}
		i = (i + sizeDirEntry) % ss // Next entry.
	}
	bcl := binary.LittleEndian.Uint32(fsys.win[i+20:]) // Bitmap cluster.
	if bcl < 2 || bcl >= fsys.n_fatent {
		return frNoFilesystem
	}
	fsys.bitbase = fsys.database + lba(uint32(fsys.csize)*(bcl-2)) // Bitmap sector.
	for {
		// Check if bitmap is contiguous.
		if fsys.move_window(fsys.fatbase+lba(bcl/(ss/4))) != frOK {
			return frDiskErr
		}
		cv := binary.LittleEndian.Uint32(fsys.win[bcl%(ss/4)*4:])
		if cv == 0xffff_ffff {
			break // Last link.
		}
		bcl++
		if cv != bcl {
			return frNoFilesystem // Fragmented bitmap.
		}
	}
	// Initialize cluster allocation information for write ops.
	fsys.last_clst = 0xffff_ffff
	fsys.free_clst = 0xffff_ffff
	fsys.fsi_flag = 0 // Enable syncing the PercInUse value in the VBR.
	fsys.fstype = fstypeExFAT
	fsys.id++ // Increment filesystem ID, invalidates open files.
	return frOK
}

// xdir_sum returns the checksum of the directory entry block in dirb.
func xdir_sum(dirb []byte) uint16 {
	szblk := (uint32(dirb[xdirNumSec]) + 1) * sizeDirEntry
	var sum uint16
	for i := uint32(0); i < szblk; i++ {
		if i == xdirSetSum {
			i++ // Skip 2-byte sum field.
		} else {
			sum = (sum>>1 | sum<<15) + uint16(dirb[i])
		}
	}
	return sum
}

// xname_sum returns the checksum (used as a hash) of the
// null-terminated UTF-16 file name.
func xname_sum(name []uint16) uint16 {
	var sum uint16
	for _, chr := range name {
		if chr == 0 {
			break
		}
		chr = uint16(ff_wtoupper(rune(chr))) // File name needs to be up-case converted.
		sum = (sum>>1 | sum<<15) + chr&0xFF
		sum = (sum>>1 | sum<<15) + chr>>8
	}
	return sum
}

// xsum32 processes one byte of the 32-bit checksum used for the up-case
// table and the VBR boot region.
func xsum32(dat byte, sum uint32) uint32 {
	return (sum>>1 | sum<<31) + uint32(dat)
}

// clusterstat_exfat is the exFAT branch of objid.clusterstat. Contiguous
// (NoFatChain) objects have no chain on the FAT: the link values are
// generated from the object size instead.
func (obj *objid) clusterstat_exfat(clst uint32) uint32 {
	fsys := obj.fs
	if (obj.objsize != 0 && obj.sclust != 0) || obj.stat == 0 {
		// Object except root dir must have valid data length.
		cofs := clst - obj.sclust                                                       // Offset from start cluster.
		clen := uint32(uint64(obj.objsize-1) / uint64(fsys.ssize) / uint64(fsys.csize)) // Number of clusters - 1.
		if obj.stat == 2 && cofs <= clen {
			// Contiguous chain: no data on the FAT, generate the value.
			if cofs == clen {
				return 0x7FFF_FFFF // Generate EOC.
			}
			return clst + 1
		}
		if obj.stat == 3 && cofs < obj.n_cont {
			return clst + 1 // In the 1st fragment: generate the value.
		}
		if obj.stat != 2 {
			// Get value from FAT if FAT chain is valid.
			if obj.n_frag != 0 {
				return 0x7FFF_FFFF // On the growing edge: generate EOC.
			}
			if fsys.move_window(fsys.fatbase+lba(clst/(uint32(fsys.ssize)/4))) != frOK {
				return badCluster
			}
			return binary.LittleEndian.Uint32(fsys.win[clst*4%uint32(fsys.ssize):]) & 0x7FFF_FFFF
		}
	}
	return 1 // Internal error.
}

// load_xdir loads the exFAT entry set (85+C0+C1s) pointed to by dp into
// fsys.dirbuf and validates its ordering and checksum.
func (dp *dir) load_xdir() (fr fileResult) {
	fsys := dp.obj.fs
	fsys.trace("dir:load_xdir")
	dirb := fsys.dirbuf[:]
	// Load file-directory entry.
	fr = fsys.move_window(dp.sect)
	if fr != frOK {
		return fr
	}
	if dp.dir[xdirType] != etFILEDIR {
		return frIntErr // Invalid order.
	}
	copy(dirb[0*sizeDirEntry:], dp.dir[:sizeDirEntry])
	szEnt := (uint32(dirb[xdirNumSec]) + 1) * sizeDirEntry // Size of this entry block.
	if szEnt < 3*sizeDirEntry || szEnt > 19*sizeDirEntry {
		return frIntErr // Invalid block size.
	}
	// Load stream extension entry.
	fr = dp.next(false)
	if fr == frNoFile {
		fr = frIntErr // It cannot be.
	}
	if fr != frOK {
		return fr
	}
	fr = fsys.move_window(dp.sect)
	if fr != frOK {
		return fr
	}
	if dp.dir[xdirType] != etSTREAM {
		return frIntErr // Invalid order.
	}
	copy(dirb[1*sizeDirEntry:], dp.dir[:sizeDirEntry])
	if maxdirb(uint32(dirb[xdirNumName])) > szEnt {
		return frIntErr // Invalid block size for the name.
	}
	// Load file name entries.
	i := uint32(2 * sizeDirEntry) // Name offset to load.
	for {
		fr = dp.next(false)
		if fr == frNoFile {
			fr = frIntErr // It cannot be.
		}
		if fr != frOK {
			return fr
		}
		fr = fsys.move_window(dp.sect)
		if fr != frOK {
			return fr
		}
		if dp.dir[xdirType] != etFILENAME {
			return frIntErr // Invalid order.
		}
		if i < maxdirb(lfnBufSize) {
			copy(dirb[i:], dp.dir[:sizeDirEntry])
		}
		i += sizeDirEntry
		if i >= szEnt {
			break
		}
	}
	// Sanity check (only for an accessible object).
	if i <= maxdirb(lfnBufSize) && xdir_sum(dirb) != binary.LittleEndian.Uint16(dirb[xdirSetSum:]) {
		return frIntErr
	}
	return frOK
}

// load_obj_xdir loads the entry set of obj into fsys.dirbuf using the
// containing-directory info recorded in obj, leaving dp pointing at it.
func (dp *dir) load_obj_xdir(obj *objid) (fr fileResult) {
	dp.obj.fs = obj.fs
	dp.obj.sclust = obj.c_scl
	dp.obj.stat = byte(obj.c_size)
	dp.obj.objsize = int64(obj.c_size & 0xFFFFFF00)
	dp.obj.n_frag = 0
	dp.blk_ofs = obj.c_ofs

	fr = dp.sdi(dp.blk_ofs) // Goto object's entry block.
	if fr == frOK {
		fr = dp.load_xdir()
	}
	return fr
}

// read_exfat is the exFAT branch of dir.read: it walks the directory to the
// next file entry set (or volume label if vol) and loads it into dirbuf.
func (dp *dir) read_exfat(vol bool) (fr fileResult) {
	fsys := dp.obj.fs
	fr = frNoFile
	for dp.sect != 0 {
		fr = fsys.move_window(dp.sect)
		if fr != frOK {
			break
		}
		et := dp.dir[xdirType]
		if et == 0 {
			fr = frNoFile // Reached end of the directory.
			break
		}
		if vol {
			if et == etVLABEL {
				break // Volume label entry.
			}
		} else if et == etFILEDIR {
			// Start of the file entry block.
			dp.blk_ofs = dp.dptr // Get location of the block.
			fr = dp.load_xdir()
			if fr == frOK {
				dp.obj.attr = fsys.dirbuf[xdirAttr] & amMASK
			}
			break
		}
		fr = dp.next(false)
		if fr != frOK {
			break
		}
	}
	if fr != frOK {
		dp.sect = 0 // Terminate the read operation on error or EOT.
	}
	return fr
}

// find_exfat is the exFAT branch of dir.find: the name to find is in
// fsys.lfnbuf. Matching is by name hash first, then case-insensitive
// UTF-16 comparison.
func (dp *dir) find_exfat() (fr fileResult) {
	fsys := dp.obj.fs
	hash := xname_sum(fsys.lfnbuf[:]) // Hash value of the name to find.
	for {
		fr = dp.read_exfat(false)
		if fr != frOK {
			break
		}
		if binary.LittleEndian.Uint16(fsys.dirbuf[xdirNameHash:]) != hash {
			continue // Skip comparison if hash mismatched.
		}
		nc := int(fsys.dirbuf[xdirNumName])
		di := sizeDirEntry * 2
		ni := 0
		for ; nc > 0; nc, di, ni = nc-1, di+2, ni+1 {
			// Compare the name.
			if di%sizeDirEntry == 0 {
				di += 2 // Skip entry type field.
			}
			wc := rune(binary.LittleEndian.Uint16(fsys.dirbuf[di:]))
			if ff_wtoupper(wc) != ff_wtoupper(rune(fsys.lfnbuf[ni])) {
				break
			}
		}
		if nc == 0 && fsys.lfnbuf[ni] == 0 {
			break // Name matched.
		}
	}
	return fr
}

// get_fileinfo_exfat fills fno from the entry set loaded in fsys.dirbuf.
func (dp *dir) get_fileinfo_exfat(fno *FileInfo) {
	fsys := dp.obj.fs
	var nc, si, di int
	var hs, wc uint16
	si = sizeDirEntry * 2 // 1st C1 entry in the entry block.
	for nc < int(fsys.dirbuf[xdirNumName]) {
		if si >= int(maxdirb(lfnBufSize)) {
			di = 0 // Truncated directory block.
			break
		}
		if si%sizeDirEntry == 0 {
			si += 2 // Skip entry type field.
		}
		wc = binary.LittleEndian.Uint16(fsys.dirbuf[si:])
		si += 2
		nc++
		if hs == 0 && isSurrogate(wc) {
			hs = wc // Get low surrogate.
			continue
		}
		r := rune(wc)
		if hs != 0 {
			r = utf16.DecodeRune(rune(hs), rune(wc))
			if r == 0xFFFD {
				di = 0 // Wrong surrogate pair.
				break
			}
		}
		nw := put_utf8(r, fno.fname[di:lfnBufSize])
		if nw == 0 {
			di = 0 // Buffer overflow or wrong char.
			break
		}
		di += nw
		hs = 0
	}
	if hs != 0 {
		di = 0 // Broken surrogate pair.
	}
	if di == 0 {
		fno.fname[di] = '?' // Inaccessible object name.
		di++
	}
	fno.fname[di] = 0  // Terminate the name.
	fno.altname[0] = 0 // exFAT does not support SFN.

	fno.fattrib = fsys.dirbuf[xdirAttr] & amMASKX
	if fno.fattrib&amDIR != 0 {
		fno.fsize = 0
	} else {
		fno.fsize = int64(binary.LittleEndian.Uint64(fsys.dirbuf[xdirFileSize:]))
	}
	fno.datetime.time = binary.LittleEndian.Uint16(fsys.dirbuf[xdirModTime:])
	fno.datetime.date = binary.LittleEndian.Uint16(fsys.dirbuf[xdirModTime+2:])
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

func (obj *objid) init_alloc_info() {
	fsys := obj.fs
	obj.sclust = binary.LittleEndian.Uint32(fsys.dirbuf[xdirFstClus:])
	obj.objsize = int64(binary.LittleEndian.Uint64(fsys.dirbuf[xdirFileSize:]))
	obj.stat = fsys.dirbuf[xdirGenFlags] & 2
	obj.n_frag = 0
}
