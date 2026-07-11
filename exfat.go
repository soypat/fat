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

// formatExFAT creates an exFAT volume spanning the whole device (SFD, no
// partition table), ported from FatFs f_mkfs. Layout: 24 VBR sectors (two
// 12-sector sets with boot checksums), FAT at sector 32, then data area
// holding the allocation bitmap, the RLE-compressed up-case table and the
// root directory. Byte-identical to a FatFs f_mkfs with FM_EXFAT|FM_SFD,
// a zero get_fattime() and erase-block size 1.
func (f *Formatter) formatExFAT(blocksize, fsSizeInBlocks int, cfg FormatConfig) error {
	const szBlk = 1 // Erase block size in sectors (data area alignment).
	ss := uint32(blocksize)
	szVol := uint32(fsSizeInBlocks)
	if szVol < 0x1000 {
		return frMkfsAborted // Too small volume for exFAT.
	}
	// Determine FAT location, data location and number of clusters.
	szAu := uint32(cfg.ClusterSize) // Cluster size in sectors.
	if szAu == 0 {
		// AU auto-selection.
		szAu = 8
		if szVol >= 0x80000 {
			szAu = 64
		}
		if szVol >= 0x4000000 {
			szAu = 256
		}
	}
	bFat := uint32(32)                                 // FAT start at offset 32.
	szFat := ((szVol/szAu+2)*4 + ss - 1) / ss          // Number of FAT sectors.
	bData := (bFat + szFat + szBlk - 1) &^ (szBlk - 1) // Align data area to the erase block boundary.
	if bData >= szVol/2 {
		return frMkfsAborted // Too small volume.
	}
	nClst := (szVol - bData) / szAu // Number of clusters.
	if nClst < 16 || nClst > clustMaxExFAT {
		return frMkfsAborted // Too few or too many clusters.
	}
	szbBit := (nClst + 7) / 8                     // Size of allocation bitmap in bytes.
	clen0 := (szbBit + szAu*ss - 1) / (szAu * ss) // Number of allocation bitmap clusters.
	win := f.window[:ss]

	// Create a compressed up-case table, streaming it to disk.
	sect := lba(bData + szAu*clen0) // Table start sector.
	var sum, szbCase, j uint32      // Table checksum to be stored in the 82 entry.
	var st int
	var si, ch uint16
	i := uint32(0)
	for {
		switch st {
		case 0:
			ch = uint16(ff_wtoupper(rune(si))) // Get an up-case char.
			if ch != si {
				si++ // Store the up-case char if exists.
				break
			}
			// Get run length of no-case block.
			for j = 1; si+uint16(j) != 0 && si+uint16(j) == uint16(ff_wtoupper(rune(si+uint16(j)))); j++ {
			}
			if j >= 128 {
				ch = 0xFFFF // Compress the no-case block if run is >= 128 chars.
				st = 2
				break
			}
			st = 1 // Do not compress short run.
			fallthrough
		case 1:
			ch = si // Fill the short run.
			si++
			j--
			if j == 0 {
				st = 0
			}
		default:
			ch = uint16(j) // Number of chars to skip.
			si += uint16(j)
			st = 0
		}
		// Put it into the write buffer.
		win[i] = byte(ch)
		sum = xsum32(byte(ch), sum)
		win[i+1] = byte(ch >> 8)
		sum = xsum32(byte(ch>>8), sum)
		i += 2
		szbCase += 2
		if si == 0 || i == ss {
			// Write buffered data when buffer full or end of process,
			// zero-padding the final partial sector.
			for k := i; k < ss; k++ {
				win[k] = 0
			}
			if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
				return err
			}
			sect++
			i = 0
		}
		if si == 0 {
			break
		}
	}
	clen1 := (szbCase + szAu*ss - 1) / (szAu * ss) // Number of up-case table clusters.
	clen2 := uint32(1)                             // Number of root directory clusters.

	// Initialize the allocation bitmap: mark clusters in use by the system
	// (bitmap, up-case and root dir).
	sect = lba(bData)
	nsect := (szbBit + ss - 1) / ss
	nbit := clen0 + clen1 + clen2
	for nsect > 0 {
		for k := range win {
			win[k] = 0
		}
		for i = 0; nbit != 0 && i/8 < ss; i, nbit = i+1, nbit-1 {
			win[i/8] |= 1 << (i % 8)
		}
		if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
			return err
		}
		sect++
		nsect--
	}

	// Initialize the FAT: chains of bitmap, up-case table and root directory.
	sect = lba(bFat)
	nsect = szFat
	b0 := clen0
	b1 := clen0 + clen1
	b2 := clen0 + clen1 + clen2
	clu := uint32(0)
	for nsect > 0 {
		for k := range win {
			win[k] = 0
		}
		for i = 0; i < ss; i, clu = i+4, clu+1 {
			var val uint32
			switch {
			case clu == 0:
				val = 0xFFFFFFF8 // FAT[0]
			case clu == 1:
				val = 0xFFFFFFFF // FAT[1]
			case clu-2 < b2:
				// System cluster chains: link sequentially, EOC at the
				// end of each chain.
				c := clu - 2
				if c == b0-1 || c == b1-1 || c == b2-1 {
					val = 0xFFFFFFFF
				} else {
					val = clu + 1
				}
			}
			binary.LittleEndian.PutUint32(win[i:], val)
		}
		if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
			return err
		}
		sect++
		nsect--
	}

	// Initialize the root directory: volume label (none), bitmap and
	// up-case table entries; the rest of the cluster is zero-filled.
	sect = lba(bData + szAu*(clen0+clen1))
	nsect = szAu
	for k := range win {
		win[k] = 0
	}
	win[sizeDirEntry*0+0] = etVLABEL                                // Volume label entry (no label).
	win[sizeDirEntry*1+0] = etBITMAP                                // Bitmap entry.
	binary.LittleEndian.PutUint32(win[sizeDirEntry*1+20:], 2)       // cluster
	binary.LittleEndian.PutUint32(win[sizeDirEntry*1+24:], szbBit)  // size
	win[sizeDirEntry*2+0] = etUPCASE                                // Up-case table entry.
	binary.LittleEndian.PutUint32(win[sizeDirEntry*2+4:], sum)      // sum
	binary.LittleEndian.PutUint32(win[sizeDirEntry*2+20:], 2+clen0) // cluster
	binary.LittleEndian.PutUint32(win[sizeDirEntry*2+24:], szbCase) // size
	for nsect > 0 {
		if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
			return err
		}
		for k := range win {
			win[k] = 0 // Rest of the entries are filled with zero.
		}
		sect++
		nsect--
	}

	// Create two sets of the exFAT VBR blocks (main and backup).
	vsn := szVol // VSN generated from volume size and creation time (zero).
	sect = 0
	for n := 0; n < 2; n++ {
		// Main record (+0).
		for k := range win {
			win[k] = 0
		}
		copy(win, "\xEB\x76\x90EXFAT   ")                                 // Boot jump code (x86), OEM name.
		binary.LittleEndian.PutUint64(win[bpbVolOfsEx:], 0)               // Volume offset in the physical drive.
		binary.LittleEndian.PutUint64(win[bpbTotSecEx:], uint64(szVol))   // Volume size in sectors.
		binary.LittleEndian.PutUint32(win[bpbFatOfsEx:], bFat)            // FAT offset.
		binary.LittleEndian.PutUint32(win[bpbFatSzEx:], szFat)            // FAT size.
		binary.LittleEndian.PutUint32(win[bpbDataOfsEx:], bData)          // Data offset.
		binary.LittleEndian.PutUint32(win[bpbNumClusEx:], nClst)          // Number of clusters.
		binary.LittleEndian.PutUint32(win[bpbRootClusEx:], 2+clen0+clen1) // Root directory cluster.
		binary.LittleEndian.PutUint32(win[bpbVolIDEx:], vsn)              // VSN.
		binary.LittleEndian.PutUint16(win[bpbFSVerEx:], 0x100)            // Filesystem version 1.00.
		for c, k := byte(0), ss; k > 1; c, k = c+1, k>>1 {
			win[bpbBytsPerSecEx] = c + 1 // Log2 of sector size.
		}
		for c, k := byte(0), szAu; k > 1; c, k = c+1, k>>1 {
			win[bpbSecPerClusEx] = c + 1 // Log2 of cluster size.
		}
		win[bpbNumFATsEx] = 1                                     // Number of FATs.
		win[bpbDrvNumEx] = 0x80                                   // Drive number (for int13).
		binary.LittleEndian.PutUint16(win[bsBootCodeEx:], 0xFEEB) // Boot code (x86 jump-to-self).
		binary.LittleEndian.PutUint16(win[510:], 0xAA55)          // Signature.
		sum = 0
		for k := uint32(0); k < ss; k++ {
			// VBR checksum; volume flags and percent-in-use are excluded.
			if k != bpbVolFlagEx && k != bpbVolFlagEx+1 && k != bpbPercInUseEx {
				sum = xsum32(win[k], sum)
			}
		}
		if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
			return err
		}
		sect++
		// Extended bootstrap records (+1..+8).
		for k := range win {
			win[k] = 0
		}
		binary.LittleEndian.PutUint16(win[ss-2:], 0xAA55) // Signature at end of sector.
		for r := 1; r < 9; r++ {
			for k := uint32(0); k < ss; k++ {
				sum = xsum32(win[k], sum)
			}
			if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
				return err
			}
			sect++
		}
		// OEM/Reserved records (+9..+10).
		for k := range win {
			win[k] = 0
		}
		for r := 9; r < 11; r++ {
			for k := uint32(0); k < ss; k++ {
				sum = xsum32(win[k], sum)
			}
			if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
				return err
			}
			sect++
		}
		// Sum record (+11): filled with the checksum value.
		for k := uint32(0); k < ss; k += 4 {
			binary.LittleEndian.PutUint32(win[k:], sum)
		}
		if _, err := f.bd.WriteBlocks(win, int64(sect)); err != nil {
			return err
		}
		sect++
	}
	return nil
}

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

// find_bitmap scans the allocation bitmap from clst for a block of ncl
// contiguous free clusters. Returns the first cluster of the block found,
// 0 if not found or badCluster on disk error.
func (fsys *FS) find_bitmap(clst, ncl uint32) uint32 {
	fsys.trace("fs:find_bitmap", slog.Uint64("clst", uint64(clst)), slog.Uint64("ncl", uint64(ncl)))
	ss := uint32(fsys.ssize)
	clst -= 2 // The first bit in the bitmap corresponds to cluster #2.
	if clst >= fsys.n_fatent-2 {
		clst = 0
	}
	scl, val := clst, clst
	var ctr uint32
	for {
		if fsys.move_window(fsys.bitbase+lba(val/8/ss)) != frOK {
			return badCluster
		}
		i := val / 8 % ss
		bm := byte(1) << (val % 8)
		for {
			for {
				bv := fsys.win[i] & bm // Get bit value.
				bm <<= 1
				val++
				if val >= fsys.n_fatent-2 {
					// Next cluster with wrap-around.
					val = 0
					bm = 0
					i = ss
				}
				if bv == 0 {
					// A free cluster: check if run length is sufficient.
					ctr++
					if ctr == ncl {
						return scl + 2
					}
				} else {
					// Encountered a cluster in-use, restart the scan.
					scl = val
					ctr = 0
				}
				if val == clst {
					return 0 // All clusters scanned.
				}
				if bm == 0 {
					break
				}
			}
			bm = 1
			i++
			if i >= ss {
				break
			}
		}
		runtime.Gosched()
	}
}

// fill_first_frag materializes the first fragment of the FAT chain of an
// object that fragmented in this session (stat 3), validating its chain.
func (obj *objid) fill_first_frag() fileResult {
	if obj.stat == 3 {
		// Create cluster chain on the FAT.
		cl := obj.sclust
		for n := obj.n_cont; n > 0; n-- {
			fr := obj.fs.put_clusterstat(cl, cl+1)
			if fr != frOK {
				return fr
			}
			cl++
		}
		obj.stat = 0 // Change status 'FAT chain is valid'.
	}
	return frOK
}

// fill_last_frag writes the pending last fragment of the object's FAT chain,
// terminating it at lcl with the value term.
func (obj *objid) fill_last_frag(lcl, term uint32) fileResult {
	for obj.n_frag > 0 {
		// Create the chain of the last fragment.
		val := term
		if obj.n_frag > 1 {
			val = lcl - obj.n_frag + 2
		}
		fr := obj.fs.put_clusterstat(lcl-obj.n_frag+1, val)
		if fr != frOK {
			return fr
		}
		obj.n_frag--
	}
	return frOK
}

// create_chain_exfat is the exFAT allocation branch of objid.create_chain:
// allocate on the bitmap and maintain the contiguity state (stat/n_cont/
// n_frag) instead of eagerly linking the FAT. scl is the cluster to start
// the free-cluster scan from. Returns the new cluster (or 0/1/badCluster
// error statuses) plus the result of any FAT writes for the shared tail of
// create_chain.
func (obj *objid) create_chain_exfat(clst, scl uint32) (uint32, fileResult) {
	fsys := obj.fs
	ncl := fsys.find_bitmap(scl, 1) // Find a free cluster.
	if ncl == 0 || ncl == badCluster {
		return ncl, frOK // No free cluster or hard error.
	}
	fr := fsys.change_bitmap(ncl, 1, true) // Mark the cluster 'in use'.
	if fr == frIntErr {
		return 1, frOK
	} else if fr == frDiskErr {
		return badCluster, frOK
	}
	if clst == 0 {
		obj.stat = 2 // New chain: set status 'contiguous'.
	} else if obj.stat == 2 && ncl != scl+1 {
		// Stretched chain got fragmented.
		obj.n_cont = scl - obj.sclust // Size of the contiguous part.
		obj.stat = 3                  // Change status 'just fragmented'.
	}
	if obj.stat != 2 {
		// The file is non-contiguous.
		if ncl == clst+1 {
			// Cluster next to previous one: increment size of last fragment.
			if obj.n_frag != 0 {
				obj.n_frag++
			} else {
				obj.n_frag = 2
			}
		} else {
			// New fragment: fill last fragment on the FAT and link it.
			if obj.n_frag == 0 {
				obj.n_frag = 1
			}
			fr = obj.fill_last_frag(clst, ncl)
			if fr == frOK {
				obj.n_frag = 1
			}
		}
	}
	return ncl, fr
}

// remove_chain_exfat_post updates the object's chain status after
// remove_chain shortened or removed its chain on an exFAT volume.
func (obj *objid) remove_chain_exfat_post(pclst uint32) fileResult {
	if pclst == 0 {
		// The entire chain has been removed.
		obj.stat = 0 // Change the chain status 'initial'.
		return frOK
	}
	if obj.stat == 0 {
		// Fragmented chain from the beginning of this session:
		// follow the chain to check if it got contiguous.
		clst := obj.sclust
		for clst != pclst {
			nxt := obj.clusterstat(clst)
			if nxt < 2 {
				return frIntErr
			}
			if nxt == badCluster {
				return frDiskErr
			}
			if nxt != clst+1 {
				break // Not contiguous.
			}
			clst++
		}
		if clst == pclst {
			obj.stat = 2 // The chain got contiguous again.
		}
	} else if obj.stat == 3 && pclst >= obj.sclust && pclst <= obj.sclust+obj.n_cont {
		// Chain fragmented in this session and got contiguous again.
		obj.stat = 2
	}
	return frOK
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

// store_xdir writes the entry set in fsys.dirbuf back to the directory at
// dp.blk_ofs, recomputing the set checksum.
func (dp *dir) store_xdir() (fr fileResult) {
	fsys := dp.obj.fs
	fsys.trace("dir:store_xdir")
	dirb := fsys.dirbuf[:]
	binary.LittleEndian.PutUint16(dirb[xdirSetSum:], xdir_sum(dirb)) // Create checksum.

	// Store the entry set to the directory.
	nent := int(dirb[xdirNumSec]) + 1
	fr = dp.sdi(dp.blk_ofs) // Top of the entry set.
	for fr == frOK {
		fr = fsys.move_window(dp.sect)
		if fr != frOK {
			break
		}
		copy(dp.dir[:sizeDirEntry], dirb[:sizeDirEntry])
		fsys.wflag = 1
		nent--
		if nent == 0 {
			break
		}
		dirb = dirb[sizeDirEntry:]
		fr = dp.next(false)
	}
	if fr == frOK || fr == frDiskErr {
		return fr
	}
	return frIntErr
}

// create_xdir builds a new entry set (85+C0+C1s) for the null-terminated
// UTF-16 name lfn into dirb, with name length, secondary count and name hash
// filled in. Times, attributes and allocation info are set by the caller.
func create_xdir(dirb []byte, lfn []uint16) {
	// Create file-directory and stream-extension entry (1st and 2nd entry).
	for i := 0; i < 2*sizeDirEntry; i++ {
		dirb[i] = 0
	}
	dirb[0*sizeDirEntry+xdirType] = etFILEDIR
	dirb[1*sizeDirEntry+xdirType] = etSTREAM

	// Create file name entries (3rd entry and follows).
	i := 2 * sizeDirEntry
	var nlen, nc1 byte
	chr := uint16(1)
	for {
		dirb[i] = etFILENAME
		dirb[i+1] = 0
		i += 2
		for {
			// Fill name field, padding with zeros once the name ends.
			if chr != 0 {
				chr = lfn[nlen]
				if chr != 0 {
					nlen++
				}
			}
			binary.LittleEndian.PutUint16(dirb[i:], chr)
			i += 2
			if i%sizeDirEntry == 0 {
				break
			}
		}
		nc1++
		if lfn[nlen] == 0 {
			break // No more C1 entries needed.
		}
	}
	dirb[xdirNumName] = nlen                                           // Set name length.
	dirb[xdirNumSec] = 1 + nc1                                         // Set secondary count (C0 + C1s).
	binary.LittleEndian.PutUint16(dirb[xdirNameHash:], xname_sum(lfn)) // Set name hash.
}

// register_exfat is the exFAT branch of dir.register: allocate directory
// entries for the name in fsys.lfnbuf and build the new entry set in
// fsys.dirbuf (written to disk later by store_xdir).
func (dp *dir) register_exfat() (fr fileResult) {
	fsys := dp.obj.fs
	nlen := fsys.lfnlen()
	nent := (nlen+14)/15 + 2 // Number of entries to allocate (85+C0+C1s).
	fr = dp.alloc(nent)
	if fr != frOK {
		return fr
	}
	dp.blk_ofs = dp.dptr - sizeDirEntry*uint32(nent-1) // Set the allocated entry block offset.

	if dp.obj.stat&4 != 0 {
		// The directory has been stretched by the new allocation.
		dp.obj.stat &^= 4
		fr = dp.obj.fill_first_frag() // Fill the first fragment on the FAT if needed.
		if fr != frOK {
			return fr
		}
		fr = dp.obj.fill_last_frag(dp.clust, badCluster) // Fill the last fragment on the FAT if needed.
		if fr != frOK {
			return fr
		}
		if dp.obj.sclust != 0 {
			// Sub-directory: update the size in its own entry set.
			var dj dir
			fr = dj.load_obj_xdir(&dp.obj) // Load the object status.
			if fr != frOK {
				return fr
			}
			dp.obj.objsize += int64(fsys.csize) * int64(fsys.ssize) // Increase directory size by cluster size.
			binary.LittleEndian.PutUint64(fsys.dirbuf[xdirFileSize:], uint64(dp.obj.objsize))
			binary.LittleEndian.PutUint64(fsys.dirbuf[xdirValidFileSize:], uint64(dp.obj.objsize))
			fsys.dirbuf[xdirGenFlags] = dp.obj.stat | 1 // Update the allocation status.
			fr = dj.store_xdir()                        // Store the object status.
			if fr != frOK {
				return fr
			}
		}
	}

	create_xdir(fsys.dirbuf[:], fsys.lfnbuf[:]) // Create on-memory directory block to be written later.
	return frOK
}

// f_sync_exfat is the directory-entry update branch of f_sync: it flushes
// pending FAT fragments and stores the file's entry set with updated
// allocation info, sizes and times.
func (fsys *FS) f_sync_exfat(fp *File, tm uint32) (fr fileResult) {
	fr = fp.obj.fill_first_frag() // Fill first fragment on the FAT if needed.
	if fr == frOK {
		fr = fp.obj.fill_last_frag(fp.clust, badCluster) // Fill last fragment on the FAT if needed.
	}
	if fr != frOK {
		return fr
	}
	var dj dir
	fr = dj.load_obj_xdir(&fp.obj) // Load directory entry block.
	if fr != frOK {
		return fr
	}
	dirb := fsys.dirbuf[:]
	dirb[xdirAttr] |= amARC              // 'file changed' attribute.
	dirb[xdirGenFlags] = fp.obj.stat | 1 // Update file allocation information.
	binary.LittleEndian.PutUint32(dirb[xdirFstClus:], fp.obj.sclust)
	binary.LittleEndian.PutUint64(dirb[xdirFileSize:], uint64(fp.obj.objsize))
	binary.LittleEndian.PutUint64(dirb[xdirValidFileSize:], uint64(fp.obj.objsize)) // Valid File Size feature unsupported: always equal.
	binary.LittleEndian.PutUint32(dirb[xdirModTime:], tm)
	dirb[xdirModTime10] = 0
	dirb[xdirModTZ] = 0
	binary.LittleEndian.PutUint32(dirb[xdirAccTime:], 0) // Invalidate last access time.
	dirb[xdirAccTZ] = 0
	fr = dj.store_xdir()
	if fr != frOK {
		return fr
	}
	return fsys.sync()
}

// open_trunc_exfat resets the entry set of the existing object found in
// fsys.dirbuf for a CreateAlways open, and removes its cluster chain.
func (fp *File) open_trunc_exfat(dj *dir, tm uint32) (res fileResult) {
	fsys := fp.obj.fs
	fp.obj.init_alloc_info() // Get current allocation info.
	// Set exFAT directory entry block initial state.
	dirb := fsys.dirbuf[:]
	for i := 2; i < 32; i++ {
		dirb[i] = 0 // Clear 85 entry except for NumSec.
	}
	for i := 38; i < 64; i++ {
		dirb[i] = 0 // Clear C0 entry except for NumName and NameHash.
	}
	dirb[xdirAttr] = amARC
	binary.LittleEndian.PutUint32(dirb[xdirCrtTime:], tm)
	binary.LittleEndian.PutUint32(dirb[xdirModTime:], tm)
	dirb[xdirGenFlags] = 1
	res = dj.store_xdir()
	if res == frOK && fp.obj.sclust != 0 {
		// Remove the cluster chain if it exists.
		res = fp.obj.remove_chain(fp.obj.sclust, 0)
		fsys.last_clst = fp.obj.sclust - 1 // Reuse the cluster hole.
	}
	return res
}

// mkdir_fin_exfat initializes the entry set of a directory just registered
// by register_exfat and stores it. dcl is the directory table cluster.
func (dj *dir) mkdir_fin_exfat(dcl, tm uint32) fileResult {
	fsys := dj.obj.fs
	dirb := fsys.dirbuf[:]
	binary.LittleEndian.PutUint32(dirb[xdirCrtTime:], tm) // Created time.
	binary.LittleEndian.PutUint32(dirb[xdirModTime:], tm)
	binary.LittleEndian.PutUint32(dirb[xdirFstClus:], dcl) // Table start cluster.
	szb := uint64(fsys.csize) * uint64(fsys.ssize)
	binary.LittleEndian.PutUint64(dirb[xdirFileSize:], szb) // Directory size needs to be valid.
	binary.LittleEndian.PutUint64(dirb[xdirValidFileSize:], szb)
	dirb[xdirGenFlags] = 3 // Contiguous, allocation possible.
	dirb[xdirAttr] = amDIR // Attribute.
	return dj.store_xdir()
}

// rename_restore_exfat restores the saved 85+C0 pair of the renamed object
// into the freshly registered entry set, keeping the new name length,
// secondary count and name hash, then stores the set.
func (djn *dir) rename_restore_exfat(buf *[2 * sizeDirEntry]byte) fileResult {
	fsys := djn.obj.fs
	dirb := fsys.dirbuf[:]
	nf, nn := dirb[xdirNumSec], dirb[xdirNumName] // Save name length and hash.
	nh := binary.LittleEndian.Uint16(dirb[xdirNameHash:])
	copy(dirb[:2*sizeDirEntry], buf[:]) // Restore 85+C0 entry.
	dirb[xdirNumSec], dirb[xdirNumName] = nf, nn
	binary.LittleEndian.PutUint16(dirb[xdirNameHash:], nh)
	if dirb[xdirAttr]&amDIR == 0 {
		dirb[xdirAttr] |= amARC // Set archive attribute if it is a file.
	}
	return djn.store_xdir()
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
