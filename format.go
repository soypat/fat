package fat

import (
	"encoding/binary"
	"errors"
	"strconv"
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

type FormatConfig struct {
	Label string
	// ClusterSize is the size of a FAT cluster in blocks.
	ClusterSize int
	// Format selects the FAT format to use. If not specified will use FAT32.
	Format Format
	// Number of reserved blocks for FAT tables. Either 1 or 2. 0 defaults to 2.
	// NumberOfFATs uint8
}

func (f *Formatter) Format(bd BlockDevice, blocksize, fsSizeInBlocks int, cfg FormatConfig) error {
	if cfg.Format == 0 {
		cfg.Format = FormatFAT32
	}
	if blocksize <= 512 || fsSizeInBlocks <= 32 || bd == nil || cfg.Format != FormatFAT32 {
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
		return frUnsupported
	default:
		return frUnsupported
	}
}

func (f *Formatter) formatFAT(bd BlockDevice, blocksize, fsSizeInBlocks int, cfg FormatConfig) error {
	const (
		fmAny byte = 1 << 7
		fmSFD byte = 1 << 6
	)
	// var (
	// 	sz_blk       = blocksize
	// 	ss           = blocksize
	// 	fsopt        = fmAny | fmSFD
	// 	n_fat  uint8 = 2
	// 	sz_au        = cfg.ClusterSize
	// )
	// if !(sz_au <= 0x1000000 && sz_au&(sz_au-1) == 0) {
	// 	return errors.New("invalid cluster size")
	// }

	// for {
	// 	pau := sz
	// }
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

// biosParamBlock a.k.a BPB is the BIOS Parameter Block for FAT32 volumes.
// It provides details on the filesystem type (FAT12, FAT16, FAT32),
// sectors per cluster, total sectors, FAT size, and more, which are essential
// for understanding the filesystem layout and capacity.
type biosParamBlock struct {
	data []byte
}

// SectorSize returns the size of a sector in bytes.
func (bs *biosParamBlock) SectorSize() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbBytsPerSec:])
}

// SetSectorSize sets the size of a sector in bytes.
func (bs *biosParamBlock) SetSectorSize(size uint16) {
	binary.LittleEndian.PutUint16(bs.data[bpbBytsPerSec:], size)
}

// SectorsPerFAT returns the number of sectors per File Allocation Table.
func (bs *biosParamBlock) SectorsPerFAT() uint32 {
	fatsz := uint32(binary.LittleEndian.Uint16(bs.data[bpbFATSz16:]))
	if fatsz == 0 {
		fatsz = binary.LittleEndian.Uint32(bs.data[bpbFATSz32:])
	}
	return fatsz
}

// SetSectorsPerFAT sets the number of sectors per File Allocation Table.
func (bs *biosParamBlock) SetSectorsPerFAT(fatsz uint32) {
	binary.LittleEndian.PutUint16(bs.data[bpbFATSz16:], 0)
	binary.LittleEndian.PutUint32(bs.data[bpbFATSz32:], fatsz)
}

// NumberOfFATs returns the number of File Allocation Tables. Should be 1 or 2.
func (bs *biosParamBlock) NumberOfFATs() uint8 {
	return bs.data[bpbNumFATs]
}

// SetNumberOfFATs sets the number of FATs.
func (bs *biosParamBlock) SetNumberOfFATs(nfats uint8) {
	bs.data[bpbNumFATs] = nfats
}

// SectorsPerCluster returns the number of sectors per cluster.
// Should be a power of 2 and not larger than 128.
func (bs *biosParamBlock) SectorsPerCluster() uint16 {
	return uint16(bs.data[bpbSecPerClus])
}

// SetSectorsPerCluster sets the number of sectors per cluster. Should be power of 2.
func (bs *biosParamBlock) SetSectorsPerCluster(spclus uint16) {
	bs.data[bpbSecPerClus] = byte(spclus)
}

// ReservedSectors returns the number of reserved sectors at the beginning of the volume.
// Should be at least 1. Reserved sectors include the boot sector, FS information sector and
// redundant sectors with these first two. The number of reserved sectors is usually
// 32 for FAT32 systems (~16k for 512 byte sectors).
// Sectors 6 and 7 are usually the backup boot sector and the FS information sector, respectively.
func (bs *biosParamBlock) ReservedSectors() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbRsvdSecCnt:])
}

// SetReservedSectors sets the number of reserved sectors at the beginning of the volume.
func (bs *biosParamBlock) SetReservedSectors(rsvd uint16) {
	binary.LittleEndian.PutUint16(bs.data[bpbRsvdSecCnt:], rsvd)
}

// TotalSectors returns the total number of sectors in the volume that
// can be used by the filesystem.
func (bs *biosParamBlock) TotalSectors() uint32 {
	totsec := uint32(binary.LittleEndian.Uint16(bs.data[bpbTotSec16:]))
	if totsec == 0 {
		totsec = binary.LittleEndian.Uint32(bs.data[bpbTotSec32:])
	}
	return totsec
}

// SetTotalSectors sets the total number of sectors in the volume that
// can be used by the filesystem.
func (bs *biosParamBlock) SetTotalSectors(totsec uint32) {
	binary.LittleEndian.PutUint16(bs.data[bpbTotSec16:], 0)
	binary.LittleEndian.PutUint32(bs.data[bpbTotSec32:], totsec)
}

// RootDirEntries returns the number of sectors occupied by the root directory.
// Should be divisible by SectorSize/32.
func (bs *biosParamBlock) RootDirEntries() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbRootEntCnt:])
}

// SetRootDirEntries sets the number of sectors occupied by the root directory.
func (bs *biosParamBlock) SetRootDirEntries(entries uint16) {
	binary.LittleEndian.PutUint16(bs.data[bpbRootEntCnt:], entries)
}

// RootCluster returns the first cluster of the root directory.
func (bs *biosParamBlock) RootCluster() uint32 {
	return binary.LittleEndian.Uint32(bs.data[bpbRootClus32:])
}

// SetRootCluster sets the first cluster of the root directory.
func (bs *biosParamBlock) SetRootCluster(cluster uint32) {
	binary.LittleEndian.PutUint32(bs.data[bpbRootClus32:], cluster)
}

// Version returns the filesystem version, should be 0.0 for FAT32.
func (bs *biosParamBlock) Version() (major, minor uint8) {
	return bs.data[bpbFSVer32], bs.data[bpbFSVer32+1]
}

func (bs *biosParamBlock) ExtendedBootSignature() uint8 {
	return bs.data[bsBootSig32]
}

// BootSignature returns the boot signature at offset 510 which should be 0xAA55.
func (bs *biosParamBlock) BootSignature() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bs55AA:])
}

// FSInfo returns the sector number of the FS Information Sector.
// Expect =1 for FAT32.
func (bs *biosParamBlock) FSInfo() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbFSInfo32:])
}

// DriveNumber returns the drive number.
func (bs *biosParamBlock) DriveNumber() uint8 {
	return bs.data[bsDrvNum32]
}

// VolumeSerialNumber returns the volume serial number.
func (bs *biosParamBlock) VolumeSerialNumber() uint32 {
	return binary.LittleEndian.Uint32(bs.data[bsVolID32:])
}

// VolumeLabel returns the volume label string.
func (bs *biosParamBlock) VolumeLabel() [11]byte {
	var label [11]byte
	copy(label[:], bs.data[bsVolLab32:])
	return label
}

func (bs *biosParamBlock) SetVolumeLabel(label string) {
	n := copy(bs.data[bsVolLab32:bsVolLab32+11], label)
	for i := n; i < 11; i++ {
		bs.data[bsVolLab32+i] = ' '
	}
}

// FilesystemType returns the filesystem type string, usually "FAT32   ".
func (bs *biosParamBlock) FilesystemType() [8]byte {
	var label [8]byte
	copy(label[:], bs.data[bsFilSysType32:])
	return label
}

// JumpInstruction returns the x86 jump instruction at the beginning of the boot sector.
func (bs *biosParamBlock) JumpInstruction() [3]byte {
	var jmpboot [3]byte
	copy(jmpboot[:], bs.data[0:])
	return jmpboot
}

// OEMName returns the Original Equipment Manufacturer name at the start of the bootsector.
func (bs *biosParamBlock) OEMName() [8]byte {
	var oemname [8]byte
	copy(oemname[:], bs.data[bsOEMName:])
	return oemname
}

// SetOEMName sets the Original Equipment Manufacturer name at the start of the bootsector.
// Will clip off any characters beyond the 8th.
func (bs *biosParamBlock) SetOEMName(name string) {
	n := copy(bs.data[bsOEMName:bsOEMName+8], name)
	for i := n; i < 8; i++ {
		bs.data[bsOEMName+i] = ' '
	}
}

func (bs *biosParamBlock) VolumeOffset() uint32 {
	return binary.LittleEndian.Uint32(bs.data[bpbHiddSec:])
}

func (bs *biosParamBlock) String() string {
	return string(bs.Appendf(nil, '\n'))
}

func labelAppend(dst []byte, label string, data []byte, sep byte) []byte {
	if len(data) == 0 {
		return dst
	}
	dst = append(dst, label...)
	dst = append(dst, ':')
	dst = append(dst, data...)
	dst = append(dst, sep)
	return dst
}

func labelAppendUint(label string, dst []byte, data uint64, sep byte) []byte {
	dst = append(dst, label...)
	dst = append(dst, ':')
	dst = strconv.AppendUint(dst, data, 10)
	dst = append(dst, sep)
	return dst
}

func labelAppendUint32(label string, dst []byte, data uint32, sep byte) []byte {
	return labelAppendUint(label, dst, uint64(data), sep)
}

func (bs *biosParamBlock) Appendf(dst []byte, separator byte) []byte {
	appendData := func(name string, data []byte, sep byte) {
		dst = labelAppend(dst, name, data, sep)
	}
	appendInt := func(name string, data uint32, sep byte) {
		dst = labelAppendUint32(name, dst, data, sep)
	}
	oem := bs.OEMName()
	appendData("OEM", clipname(oem[:]), separator)
	fstype := bs.FilesystemType()
	appendData("FSType", clipname(fstype[:]), separator)
	volLabel := bs.VolumeLabel()
	appendData("VolumeLabel", clipname(volLabel[:]), separator)
	appendInt("VolumeSerialNumber", bs.VolumeSerialNumber(), separator)
	appendInt("VolumeOffset", bs.VolumeOffset(), separator)
	appendInt("SectorSize", uint32(bs.SectorSize()), separator)
	appendInt("SectorsPerCluster", uint32(bs.SectorsPerCluster()), separator)
	appendInt("ReservedSectors", uint32(bs.ReservedSectors()), separator)
	appendInt("NumberOfFATs", uint32(bs.NumberOfFATs()), separator)
	appendInt("RootDirEntries", uint32(bs.RootDirEntries()), separator)
	appendInt("TotalSectors", uint32(bs.TotalSectors()), separator)
	appendInt("SectorsPerFAT", uint32(bs.SectorsPerFAT()), separator)
	appendInt("RootCluster", uint32(bs.RootCluster()), separator)
	appendInt("FSInfo", uint32(bs.FSInfo()), separator)
	appendInt("DriveNumber", uint32(bs.DriveNumber()), separator)
	major, minor := bs.Version()
	if major != 0 || minor != 0 {
		appendInt("Version", uint32(major)<<16|uint32(minor), separator)
	}
	// appendData("BootCode", bstr(bs.BootCode()), separator)
	return dst
}

// bootcode returns the boot code at the end of the boot sector.
func (bs *biosParamBlock) bootcode() []byte {
	return bs.data[bsBootCode32:bs55AA]
}

// fsinfoSector is the FS Information Sector for FAT32 volumes.
type fsinfoSector struct {
	data []byte
}

// Signatures returns the 3 signatures at the beginning, middle and end of the sector.
// Expect them to be 0x41615252, 0x61417272, 0xAA550000 respectively.
func (fsi *fsinfoSector) Signatures() (sigStart, sigMid, sigEnd uint32) {
	return binary.LittleEndian.Uint32(fsi.data[0:]),
		binary.LittleEndian.Uint32(fsi.data[0x1e4:]),
		binary.LittleEndian.Uint32(fsi.data[0x1fc:])
}

// SetSignatures sets the 3 signatures at the beginning, middle and end of the sector.
// Should be called as follows to set valid signatures expected by most implementations:
//
//	fsi.SetSignatures(0x41615252, 0x61417272, 0xAA550000)
func (fsi *fsinfoSector) SetSignatures(sigStart, sigMid, sigEnd uint32) {
	binary.LittleEndian.PutUint32(fsi.data[0:], sigStart)
	binary.LittleEndian.PutUint32(fsi.data[0x1e4:], sigMid)
	binary.LittleEndian.PutUint32(fsi.data[0x1fc:], sigEnd)
}

// FreeClusterCount is the last known number of free data clusters on the volume,
// or 0xFFFFFFFF if unknown. Should be set to 0xFFFFFFFF during format and updated by
// the operating system later on. Must not be absolutely relied upon to be correct in all scenarios.
// Before using this value, the operating system should sanity check this value to
// be less than or equal to the volume's count of clusters.
func (fsi *fsinfoSector) FreeClusterCount() uint32 {
	return binary.LittleEndian.Uint32(fsi.data[0x1e8:])
}

// SetFreeClusterCount sets the last known number of free data clusters on the volume.
func (fsi *fsinfoSector) SetFreeClusterCount(count uint32) {
	binary.LittleEndian.PutUint32(fsi.data[0x1e8:], count)
}

// LastAllocatedCluster is the number of the most recently known to be allocated data cluster.
// Should be set to 0xFFFFFFFF during format and updated by the operating system later on.
// With 0xFFFFFFFF the system should start at cluster 0x00000002. Must not be absolutely
// relied upon to be correct in all scenarios. Before using this value, the operating system
// should sanity check this value to be a valid cluster number on the volume.
func (fsi *fsinfoSector) LastAllocatedCluster() uint32 {
	return binary.LittleEndian.Uint32(fsi.data[0x1ec:])
}

// SetLastAllocatedCluster sets the number of the most recently known to be allocated data cluster.
func (fsi *fsinfoSector) SetLastAllocatedCluster(cluster uint32) {
	binary.LittleEndian.PutUint32(fsi.data[0x1ec:], cluster)
}

func (fsi *fsinfoSector) String() string {
	return string(fsi.Appendf(nil, '\n'))
}

func (fsi *fsinfoSector) Appendf(dst []byte, separator byte) []byte {
	lo, mid, hi := fsi.Signatures()
	if lo != 0x41615252 || mid != 0x61417272 || hi != 0xAA550000 {
		dst = append(dst, "invalid fsi signatures"...)
		dst = append(dst, separator)
	}
	dst = labelAppendUint32("FreeClusterCount", dst, fsi.FreeClusterCount(), separator)
	dst = labelAppendUint32("LastAllocatedCluster", dst, fsi.LastAllocatedCluster(), separator)
	return dst
}

// fatSector is a File Allocation Table sector.
type fat32Sector struct {
	data []byte
}

type entry uint32

func (fs *fat32Sector) Entry(idx int) entry {
	return entry(binary.LittleEndian.Uint32(fs.data[idx*4:]))
}

func (fs *fat32Sector) SetEntry(idx int, ent entry) {
	binary.LittleEndian.PutUint32(fs.data[idx*4:], uint32(ent))
}

func (fs entry) Cluster() uint32 {
	return uint32(fs) & 0x0FFF_FFFF
}

func (e entry) Appendf(dst []byte, separator byte) []byte {
	if e.IsEOF() {
		dst = labelAppendUint32("entry", dst, e.Cluster(), ' ')
		return append(dst, "EOF"...)
	}
	return labelAppendUint32("entry", dst, e.Cluster(), separator)
}

func (e entry) IsEOF() bool {
	return e&0x0FFF_FFFF >= 0x0FFF_FFF8
}

func (fs *fat32Sector) String() string {
	return string(fs.AppendfEntries(nil, " -> ", '\n'))
}

func (fs *fat32Sector) AppendfEntries(dst []byte, entrySep string, chainSep byte) []byte {
	var inChain bool
	for i := 0; i < len(fs.data)/4; i++ {
		entry := fs.Entry(i)
		if entry == 0 {
			break
		}
		dst = entry.Appendf(dst, chainSep)
		if entry.IsEOF() {
			dst = append(dst, chainSep)
			inChain = false
		} else if inChain {
			dst = append(dst, entrySep...)
		} else {
			inChain = true
		}
	}
	return dst
}
