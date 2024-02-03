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

type bootsector struct {
	data []byte
}

// SectorSize returns the size of a sector in bytes.
func (bs *bootsector) SectorSize() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbBytsPerSec:])
}

// SetSectorSize sets the size of a sector in bytes.
func (bs *bootsector) SetSectorSize(size uint16) {
	binary.LittleEndian.PutUint16(bs.data[bpbBytsPerSec:], size)
}

// SectorsPerFAT returns the number of sectors per File Allocation Table.
func (bs *bootsector) SectorsPerFAT() uint32 {
	fatsz := uint32(binary.LittleEndian.Uint16(bs.data[bpbFATSz16:]))
	if fatsz == 0 {
		fatsz = binary.LittleEndian.Uint32(bs.data[bpbFATSz32:])
	}
	return fatsz
}

// SetSectorsPerFAT sets the number of sectors per File Allocation Table.
func (bs *bootsector) SetSectorsPerFAT(fatsz uint32) {
	binary.LittleEndian.PutUint16(bs.data[bpbFATSz16:], 0)
	binary.LittleEndian.PutUint32(bs.data[bpbFATSz32:], fatsz)
}

// NumberOfFATs returns the number of File Allocation Tables. Should be 1 or 2.
func (bs *bootsector) NumberOfFATs() uint8 {
	return bs.data[bpbNumFATs]
}

// SetNumberOfFATs sets the number of FATs.
func (bs *bootsector) SetNumberOfFATs(nfats uint8) {
	bs.data[bpbNumFATs] = nfats
}

// SectorsPerCluster returns the number of sectors per cluster.
// Should be a power of 2 and not larger than 128.
func (bs *bootsector) SectorsPerCluster() uint16 {
	return uint16(bs.data[bpbSecPerClus])
}

// SetSectorsPerCluster sets the number of sectors per cluster. Should be power of 2.
func (bs *bootsector) SetSectorsPerCluster(spclus uint16) {
	bs.data[bpbSecPerClus] = byte(spclus)
}

// ReservedSectors returns the number of reserved sectors at the beginning of the volume.
// Should be at least 1.
func (bs *bootsector) ReservedSectors() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbRsvdSecCnt:])
}

// SetReservedSectors sets the number of reserved sectors at the beginning of the volume.
func (bs *bootsector) SetReservedSectors(rsvd uint16) {
	binary.LittleEndian.PutUint16(bs.data[bpbRsvdSecCnt:], rsvd)
}

// TotalSectors returns the total number of sectors in the volume that
// can be used by the filesystem.
func (bs *bootsector) TotalSectors() uint32 {
	totsec := uint32(binary.LittleEndian.Uint16(bs.data[bpbTotSec16:]))
	if totsec == 0 {
		totsec = binary.LittleEndian.Uint32(bs.data[bpbTotSec32:])
	}
	return totsec
}

// SetTotalSectors sets the total number of sectors in the volume that
// can be used by the filesystem.
func (bs *bootsector) SetTotalSectors(totsec uint32) {
	binary.LittleEndian.PutUint16(bs.data[bpbTotSec16:], 0)
	binary.LittleEndian.PutUint32(bs.data[bpbTotSec32:], totsec)
}

// RootDirEntries returns the number of sectors occupied by the root directory.
// Should be divisible by SectorSize/32.
func (bs *bootsector) RootDirEntries() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbRootEntCnt:])
}

// SetRootDirEntries sets the number of sectors occupied by the root directory.
func (bs *bootsector) SetRootDirEntries(entries uint16) {
	binary.LittleEndian.PutUint16(bs.data[bpbRootEntCnt:], entries)
}

// RootCluster returns the first cluster of the root directory.
func (bs *bootsector) RootCluster() uint32 {
	return binary.LittleEndian.Uint32(bs.data[bpbRootClus32:])
}

// SetRootCluster sets the first cluster of the root directory.
func (bs *bootsector) SetRootCluster(cluster uint32) {
	binary.LittleEndian.PutUint32(bs.data[bpbRootClus32:], cluster)
}

// Version returns the filesystem version, should be 0.0 for FAT32.
func (bs *bootsector) Version() (major, minor uint8) {
	return bs.data[bpbFSVer32], bs.data[bpbFSVer32+1]
}

func (bs *bootsector) ExtendedBootSignature() uint8 {
	return bs.data[bsBootSig32]
}

// BootSignature returns the boot signature at offset 510 which should be 0xAA55.
func (bs *bootsector) BootSignature() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bs55AA:])
}

// FSInfo returns the sector number of the FS Information Sector.
// Expect =1 for FAT32.
func (bs *bootsector) FSInfo() uint16 {
	return binary.LittleEndian.Uint16(bs.data[bpbFSInfo32:])
}

// DriveNumber returns the drive number.
func (bs *bootsector) DriveNumber() uint8 {
	return bs.data[bsDrvNum32]
}

// VolumeSerialNumber returns the volume serial number.
func (bs *bootsector) VolumeSerialNumber() uint32 {
	return binary.LittleEndian.Uint32(bs.data[bsVolID32:])
}

// VolumeLabel returns the volume label string.
func (bs *bootsector) VolumeLabel() [11]byte {
	var label [11]byte
	copy(label[:], bs.data[bsVolLab32:])
	return label
}

func (bs *bootsector) SetVolumeLabel(label string) {
	n := copy(bs.data[bsVolLab32:bsVolLab32+11], label)
	for i := n; i < 11; i++ {
		bs.data[bsVolLab32+i] = ' '
	}
}

// FilesystemType returns the filesystem type string, usually "FAT32   ".
func (bs *bootsector) FilesystemType() [8]byte {
	var label [8]byte
	copy(label[:], bs.data[bsFilSysType32:])
	return label
}

// JumpInstruction returns the x86 jump instruction at the beginning of the boot sector.
func (bs *bootsector) JumpInstruction() [3]byte {
	var jmpboot [3]byte
	copy(jmpboot[:], bs.data[0:])
	return jmpboot
}

// OEMName returns the Original Equipment Manufacturer name at the start of the bootsector.
func (bs *bootsector) OEMName() [8]byte {
	var oemname [8]byte
	copy(oemname[:], bs.data[bsOEMName:])
	return oemname
}

// SetOEMName sets the Original Equipment Manufacturer name at the start of the bootsector.
// Will clip off any characters beyond the 8th.
func (bs *bootsector) SetOEMName(name string) {
	n := copy(bs.data[bsOEMName:bsOEMName+8], name)
	for i := n; i < 8; i++ {
		bs.data[bsOEMName+i] = ' '
	}
}

func (bs *bootsector) VolumeOffset() uint32 {
	return binary.LittleEndian.Uint32(bs.data[bpbHiddSec:])
}

func (bs *bootsector) String() string {
	return string(bs.Appendf(nil, '\n'))
}

func (bs *bootsector) Appendf(dst []byte, separator byte) []byte {
	appendData := func(name string, data []byte, sep byte) {
		if len(data) == 0 {
			return
		}
		dst = append(dst, name...)
		dst = append(dst, ':')
		dst = append(dst, data...)
		dst = append(dst, sep)
	}
	appendInt := func(name string, data uint32, sep byte) {
		dst = append(dst, name...)
		dst = append(dst, ':')
		dst = strconv.AppendUint(dst, uint64(data), 10)
		dst = append(dst, sep)
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

// BootCode returns the boot code at the end of the boot sector.
func (bs *bootsector) BootCode() []byte {
	return bs.data[bsBootCode32:bs55AA]
}
