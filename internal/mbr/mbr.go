/*
package mbr implements a Master Boot Record parser and writer.
*/
package mbr

import (
	"encoding/binary"
	"errors"
)

const (
	bootstrapLen     = 440
	uniqueDiskIDOff  = bootstrapLen
	uniqueDiskIDLen  = 4
	reservedLen      = 2
	pteOffset        = bootstrapLen + uniqueDiskIDLen + reservedLen
	pteLen           = 16 // partition table entry length
	bootSignatureOff = 510
	BootSignature    = 0xAA55
)

// ToBootSector converts a byte slice to an MBR BootSector while maintaining a
// reference to the original byte slice. The byte slice must be at least 512
// bytes long and the first byte of the slice must be the first byte of the MBR.
func ToBootSector(start []byte) (BootSector, error) {
	if len(start) < 512 {
		return BootSector{}, errors.New("boot sector too short")
	}
	bs := BootSector{
		data: start[:512:512],
	}
	return bs, nil
}

// BootSector is a Master Boot Record. It contains the bootstrap code, the partition table and a boot signature.
type BootSector struct {
	data []byte
}

// PartitionTableEntry represents one of the four partition table entries in the MBR.
// It contains information about the partition, such as the type, size, location and if it is bootable.
// See https://en.wikipedia.org/wiki/Master_boot_record#PTE for more information.
type PartitionTableEntry struct {
	data [pteLen]byte
}

// Bootstrap returns bytes 0..439 of the MBR containing the binary executable code.
func (mbr *BootSector) Bootstrap() []byte {
	return mbr.data[0:bootstrapLen]
}

func (mbr *BootSector) UniqueDiskID() uint32 {
	return binary.LittleEndian.Uint32(mbr.data[uniqueDiskIDOff : uniqueDiskIDOff+uniqueDiskIDLen])
}

// BootSignature returns the boot signature of the MBR. This is a magic number that indicates that this is a valid MBR.
func (mbr *BootSector) BootSignature() uint16 {
	return binary.LittleEndian.Uint16(mbr.data[bootSignatureOff : bootSignatureOff+2])
}

// PartitionTable returns the idx'th partition table entry of the MBR.
func (mbr *BootSector) PartitionTable(idx int) PartitionTableEntry {
	if idx > 3 {
		panic("invalid partition table index")
	}
	return PartitionTableEntry{
		data: [pteLen]byte(mbr.data[pteOffset+idx*pteLen : pteOffset+(idx+1)*pteLen]),
	}
}

// SetPartitionTable sets the idx'th partition table entry of the MBR.
func (mbr *BootSector) SetPartitionTable(idx int, pte PartitionTableEntry) {
	if idx > 3 {
		panic("invalid partition table index")
	}
	copy(mbr.data[pteOffset+idx*pteLen:pteOffset+(idx+1)*pteLen], pte.data[:])
}

// MakePTE creates a new partition table entry from the given parameters.
func MakePTE(attrs DriveAttributes, Type PartitionType, startLBA, numLBA uint32, startCHS, lastCHS CHS) PartitionTableEntry {
	pte := PartitionTableEntry{}
	pte.data[0] = byte(attrs)
	pte.data[4] = byte(Type)
	binary.LittleEndian.PutUint32(pte.data[8:12], startLBA)
	binary.LittleEndian.PutUint32(pte.data[12:16], numLBA)
	pte.data[1], pte.data[2], pte.data[3] = startCHS.Tuple()
	pte.data[5], pte.data[6], pte.data[7] = lastCHS.Tuple()
	return pte
}

// Attributes returns the attributes of the partition the PTE refers to.
func (pte *PartitionTableEntry) Attributes() DriveAttributes {
	return DriveAttributes(pte.data[0])
}

// CHSStart returns the starting sector of the partition in CHS format. Is not used by modern operating systems.
func (pte *PartitionTableEntry) CHSStart() CHS {
	return CHS(pte.data[1]) | CHS(pte.data[2])<<8 | CHS(pte.data[3])<<16
}

// ParitionType returns the type the partition refers to, such as if the partition is
// formatted as a FAT32, NTFS, exFAT, Linux etc.
func (pte *PartitionTableEntry) PartitionType() PartitionType {
	return PartitionType(pte.data[4])
}

// CHSLast returns the last sector of the partition in CHS format.
func (pte *PartitionTableEntry) CHSLast() CHS {
	return CHS(pte.data[5]) | CHS(pte.data[6])<<8 | CHS(pte.data[7])<<16
}

// StartLBA returns the starting sector of the partition in LBA format (logical block address).
func (pte *PartitionTableEntry) StartLBA() uint32 {
	return binary.LittleEndian.Uint32(pte.data[8:12])
}

// NumberOfLBA returns the number of sectors (logical block addresses) in the partition.
func (pte *PartitionTableEntry) NumberOfLBA() uint32 {
	return binary.LittleEndian.Uint32(pte.data[12:16])
}

// IsBootable returns true if the partition the PTE refers to is bootable.
func (attrs DriveAttributes) IsBootable() bool {
	return DriveAttrsBootable&0x80 != 0
}

// CHS is a cylinder-head-sector address. This addressing scheme is deprecated by modern operating systems
// in favor of LBA, or Logical Block Addressing.
type CHS uint32

func (chs CHS) Tuple() (cylinder, head, sector uint8) {
	return uint8(chs), uint8(chs >> 8), uint8(chs >> 16)
}

// NewCHS creates a new CHS address from the cylinder, head and sector numbers. See "CHS addressing".
func NewCHS(cylinder, head, sector uint8) CHS {
	return CHS(cylinder) | CHS(head)<<8 | CHS(sector)<<16
}

// PartitionType refers to the type of partition the Partition Table Entry refers to.
type PartitionType byte

const (
	PartitionTypeUnused   PartitionType = 0x00
	PartitionTypeFAT12    PartitionType = 0x01
	PartitionTypeFAT16    PartitionType = 0x04
	PartitionTypeExtended PartitionType = 0x05
	PartitionTypeFAT32CHS PartitionType = 0x0B
	PartitionTypeFAT32LBA PartitionType = 0x0C
	PartitionTypeNTFS     PartitionType = 0x07 // Also includes exFAT.
	PartitionTypeLinux    PartitionType = 0x83
	PartitionTypeFreeBSD  PartitionType = 0xA5
	PartitionTypeAppleHFS PartitionType = 0xAF
)

// DriveAttributes refers to the first byte of a Partition Table Entry. It specifies
// if the partition is bootable.
type DriveAttributes byte

const (
	DriveAttrsBootable DriveAttributes = 0x80
)
