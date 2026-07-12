package fat

import (
	"encoding/binary"
	"errors"
)

type Format uint8

const (
	_FormatUnknown Format = iota
	FormatFAT12
	FormatFAT16
	FormatFAT32
	FormatExFAT
)

type Formatter struct {
	window     []byte
	windowaddr lba
	// block device is temporarily used by the formatter to read/write blocks.
	bd BlockDevice
}

type FormatParams struct {
	Label string
	// ClusterSize is the size of a FAT cluster in blocks.
	ClusterSize int
	// Format selects the FAT format to use. If not specified will use FAT32.
	Format Format
	// Number of reserved blocks for FAT tables. Either 1 or 2. 0 defaults to 2.
	// NumberOfFATs uint8
}

func (f *Formatter) Format(bd BlockDevice, blocksize, fsSizeInBlocks int, cfg FormatParams) error {
	if cfg.Format == 0 {
		cfg.Format = FormatFAT32
	}
	if blocksize < 512 || blocksize&(blocksize-1) != 0 || fsSizeInBlocks <= 32 || bd == nil {
		return errors.New("invalid Format argument")
	}
	if len(f.window) < blocksize {
		f.window = make([]byte, blocksize)
	}
	if cfg.Label == "" {
		cfg.Label = "tinygo.unnamed"
	}
	f.windowaddr = ^lba(0)
	f.bd = bd

	switch cfg.Format {
	case FormatFAT12, FormatFAT16, FormatFAT32:
		return f.formatFAT(bd, blocksize, fsSizeInBlocks, cfg)
	case FormatExFAT:
		return f.formatExFAT(blocksize, fsSizeInBlocks, cfg)
	default:
		return frUnsupported
	}
}

// Cluster size selection tables, from FatFs f_mkfs. The volume size is measured
// in 4K sectors for FAT12/16 and in 128K sectors for FAT32; the cluster size
// doubles past each bound, so a bigger volume gets bigger clusters and the FAT
// stays a sane size.
var (
	cstFAT   = [...]uint32{1, 4, 16, 64, 256, 512, 0}
	cstFAT32 = [...]uint32{1, 2, 4, 8, 16, 32, 0}
)

// formatFAT creates a FAT12, FAT16 or FAT32 volume spanning the whole device
// (SFD, no partition table), ported from FatFs f_mkfs. Layout: the volume boot
// record at sector 0, then — on FAT32 only — the FSInfo sector, a backup boot
// record at sector 6 and a backup FSInfo at 7; then two FATs; then the root
// directory, which on FAT12/16 is a fixed area of nRootDir entries and on FAT32
// is an ordinary one-cluster chain at cluster 2.
//
// Which of the three you get is NOT a free choice, and cfg.Format is a request
// rather than an instruction. The FAT sub-type is a function of the cluster
// count and nothing else: a driver counts the clusters and decides, so a volume
// with 65525 clusters or fewer IS FAT16 no matter what the boot record says. If
// the requested type cannot describe the volume at the chosen cluster size, and
// the cluster size was left to us, we double it and try again — that is the
// retry loop below, and it is why the cluster size can come out larger than a
// caller expects on a big volume.
//
// The consequence worth knowing: there is no such thing as a small FAT32. It
// needs more than 65525 clusters, so at the smallest cluster size (one sector)
// the volume still cannot be under about 32 MiB. Ask for FAT32 on anything
// smaller and this returns frMkfsAborted rather than quietly handing back a
// FAT16 that claims to be FAT32.
func (f *Formatter) formatFAT(bd BlockDevice, blocksize, fsSizeInBlocks int, cfg FormatParams) error {
	const (
		szBlk    = 1   // Erase block size in sectors (data area alignment).
		nFAT     = 2   // Number of FATs. FatFs' n_fat.
		nRootDir = 512 // Root directory entries on FAT12/16. FatFs' n_rootdir.
	)
	ss := uint32(blocksize)
	szVol := uint32(fsSizeInBlocks)
	szAu := uint32(cfg.ClusterSize) // Cluster size in sectors; 0 selects it below.
	if szAu&(szAu-1) != 0 || szAu > 128 {
		return errors.New("invalid cluster size: want a power of two, at most 128 sectors")
	}

	fsty := cfg.Format
	var pau, nClst, szFat, szRsv, szDir, bFat, bData uint32
	for {
		pau = szAu
		// Pre-determine the cluster count and the FAT sub-type.
		if fsty == FormatFAT32 {
			if pau == 0 {
				n := szVol / 0x20000 // Volume size in units of 128K sectors.
				pau = 1
				for i := 0; cstFAT32[i] != 0 && cstFAT32[i] <= n; i++ {
					pau <<= 1
				}
			}
			nClst = szVol / pau
			szFat = (nClst*4 + 8 + ss - 1) / ss // Four bytes per entry, plus FAT[0] and FAT[1].
			szRsv = 32                          // Room for the FSInfo and the backup boot record.
			szDir = 0                           // The root directory is a cluster chain, not an area.
			if nClst <= clustMaxFAT16 || nClst > clustMaxFAT32 {
				return frMkfsAborted
			}
		} else {
			if pau == 0 {
				n := szVol / 0x1000 // Volume size in units of 4K sectors.
				pau = 1
				for i := 0; cstFAT[i] != 0 && cstFAT[i] <= n; i++ {
					pau <<= 1
				}
			}
			nClst = szVol / pau
			var n uint32
			if nClst > clustMaxFAT12 {
				n = nClst*2 + 4 // FAT16: two bytes per entry.
			} else {
				fsty = FormatFAT12 // Too few clusters to be FAT16, whatever was asked for.
				n = (nClst*3+1)/2 + 3
			}
			szFat = (n + ss - 1) / ss
			szRsv = 1
			szDir = nRootDir * sizeDirEntry / ss
		}
		bFat = szRsv                      // FAT base. The volume starts at sector 0.
		bData = bFat + szFat*nFAT + szDir // Data base.

		// Align the data area to an erase block boundary. With szBlk 1 this is a
		// no-op, and it is kept only so the arithmetic still tracks f_mkfs if the
		// erase block size ever becomes a parameter.
		n := ((bData + szBlk - 1) &^ (szBlk - 1)) - bData
		if fsty == FormatFAT32 {
			szRsv += n // Move the FAT.
			bFat += n
		} else {
			if n%nFAT != 0 { // Expand the FAT, fixing up a fractional sector.
				n--
				szRsv++
				bFat++
			}
			szFat += n / nFAT
		}

		// Now that the areas are placed, count the clusters that actually fit and
		// check the sub-type against them for real.
		if szVol < bData+pau*16 {
			return frMkfsAborted // Too small a volume.
		}
		nClst = (szVol - szRsv - szFat*nFAT - szDir) / pau

		switch {
		case fsty == FormatFAT32 && nClst <= clustMaxFAT16:
			// Too few clusters to be FAT32. Halving the cluster size makes more of
			// them, but only if we are the one choosing it.
			if szAu == 0 {
				if szAu = pau / 2; szAu != 0 {
					continue
				}
			}
			return frMkfsAborted

		case fsty == FormatFAT16 && nClst > clustMaxFAT16:
			// Too many clusters to be FAT16. Doubling the cluster size makes fewer.
			if szAu == 0 {
				if szAu = pau * 2; szAu <= 128 {
					continue
				}
			}
			return frMkfsAborted

		case fsty == FormatFAT16 && nClst <= clustMaxFAT12:
			if szAu == 0 {
				if szAu = pau * 2; szAu <= 128 {
					continue
				}
			}
			return frMkfsAborted

		case fsty == FormatFAT12 && nClst > clustMaxFAT12:
			return frMkfsAborted // Too many clusters to be FAT12.
		}
		break // The cluster configuration is valid.
	}

	win := f.window[:ss]
	zero := func() {
		for i := range win {
			win[i] = 0
		}
	}

	// The volume boot record.
	zero()
	// The jump is a jump-to-self: this volume is not bootable, and a BIOS that
	// runs it should hang rather than execute the rest of the sector as code.
	copy(win[bsJmpBoot:], "\xEB\xFE\x90MSDOS5.0")
	binary.LittleEndian.PutUint16(win[bpbBytsPerSec:], uint16(ss))
	win[bpbSecPerClus] = byte(pau)
	binary.LittleEndian.PutUint16(win[bpbRsvdSecCnt:], uint16(szRsv))
	win[bpbNumFATs] = nFAT
	if fsty == FormatFAT32 {
		binary.LittleEndian.PutUint16(win[bpbRootEntCnt:], 0) // Must be zero, and mount rejects it otherwise.
	} else {
		binary.LittleEndian.PutUint16(win[bpbRootEntCnt:], nRootDir)
	}
	if szVol < 0x10000 {
		binary.LittleEndian.PutUint16(win[bpbTotSec16:], uint16(szVol))
	} else {
		binary.LittleEndian.PutUint32(win[bpbTotSec32:], szVol)
	}
	win[bpbMedia] = 0xF8 // Fixed disk.
	binary.LittleEndian.PutUint16(win[bpbSecPerTrk:], 63)
	binary.LittleEndian.PutUint16(win[bpbNumHeads:], 255)
	binary.LittleEndian.PutUint32(win[bpbHiddSec:], 0) // The volume is the whole drive.
	vsn := szVol                                       // Volume serial, from the size: the image has to be deterministic.
	if fsty == FormatFAT32 {
		binary.LittleEndian.PutUint32(win[bsVolID32:], vsn)
		binary.LittleEndian.PutUint32(win[bpbFATSz32:], szFat)
		binary.LittleEndian.PutUint32(win[bpbRootClus32:], 2) // The root directory is cluster 2.
		binary.LittleEndian.PutUint16(win[bpbFSInfo32:], 1)
		binary.LittleEndian.PutUint16(win[bpbBkBootSec32:], 6)
		win[bsDrvNum32] = 0x80
		win[bsBootSig32] = 0x29
		copy(win[bsVolLab32:], "NO NAME    FAT32   ") // Label, then the type string mount looks for.
	} else {
		binary.LittleEndian.PutUint32(win[bsVolID:], vsn)
		binary.LittleEndian.PutUint16(win[bpbFATSz16:], uint16(szFat))
		win[bsDrvNum] = 0x80
		win[bsBootSig] = 0x29
		copy(win[bsVolLab:], "NO NAME    FAT     ")
	}
	binary.LittleEndian.PutUint16(win[bs55AA:], 0xAA55)
	if _, err := bd.WriteBlocks(win, 0); err != nil {
		return err
	}

	// The FSInfo record, and the backups of both. FAT32 only.
	if fsty == FormatFAT32 {
		if _, err := bd.WriteBlocks(win, 6); err != nil { // Backup boot record.
			return err
		}
		zero()
		binary.LittleEndian.PutUint32(win[fsiLeadSig:], 0x41615252)
		binary.LittleEndian.PutUint32(win[fsiStrucSig:], 0x61417272)
		binary.LittleEndian.PutUint32(win[fsiFree_Count:], nClst-1) // Cluster 2 is the root directory.
		binary.LittleEndian.PutUint32(win[fsiNxt_Free:], 2)
		binary.LittleEndian.PutUint16(win[bs55AA:], 0xAA55)
		if _, err := bd.WriteBlocks(win, 7); err != nil { // Backup FSInfo.
			return err
		}
		if _, err := bd.WriteBlocks(win, 1); err != nil {
			return err
		}
	}

	// The FATs. Every entry is free, which is zero — except the two reserved ones
	// at the head, and on FAT32 the root directory's chain, which is cluster 2 and
	// ends immediately.
	sect := bFat
	for range nFAT {
		zero()
		switch fsty {
		case FormatFAT32:
			binary.LittleEndian.PutUint32(win[0:], 0xFFFFFFF8) // FAT[0]: the media byte.
			binary.LittleEndian.PutUint32(win[4:], 0xFFFFFFFF) // FAT[1]: reserved.
			binary.LittleEndian.PutUint32(win[8:], 0x0FFFFFFF) // FAT[2]: the root directory, end of chain.
		case FormatFAT12:
			binary.LittleEndian.PutUint32(win[0:], 0x00FFFFF8) // FAT[0] and FAT[1], 12 bits each.
		default:
			binary.LittleEndian.PutUint32(win[0:], 0xFFFFFFF8) // FAT[0] and FAT[1], 16 bits each.
		}
		for n := szFat; n > 0; n-- {
			if _, err := bd.WriteBlocks(win, int64(sect)); err != nil {
				return err
			}
			zero() // Only the first sector of a FAT has anything in it.
			sect++
		}
	}

	// The root directory, zero-filled: a directory entry that begins with a zero
	// byte is the end of the directory, so a cleared area is an empty one. On
	// FAT32 this is cluster 2, which is the first cluster of the data area and so
	// is exactly where sect has arrived.
	nsect := szDir
	if fsty == FormatFAT32 {
		nsect = pau
	}
	for ; nsect > 0; nsect-- {
		if _, err := bd.WriteBlocks(win, int64(sect)); err != nil {
			return err
		}
		sect++
	}
	return nil
}

func (f *Formatter) move_window(addr lba) error {
	if addr != f.windowaddr {
		if _, err := f.bd.ReadBlocks(f.window, int64(addr)); err != nil {
			return err
		}
		f.windowaddr = addr
	}
	return nil
}
