package gpt

import (
	"encoding/binary"
	"errors"

	"github.com/soypat/fat/internal/utf16x"
)

const (
	pteNameOff = 56
	pteNameLen = 72
)

type Header struct {
	data []byte
}

func ToHeader(start []byte) (Header, error) {
	if len(start) < 92 {
		return Header{}, errors.New("gpt header too short")
	}
	h := Header{
		data: start[:92:92],
	}
	return h, nil
}

// Signature returns the 8-byte signature at the start of the GPT header.
// Expect it to be 0x5452415020494645, which is "EFI PART" in little-endian.
func (h *Header) Signature() (sig uint64) {
	return binary.LittleEndian.Uint64(h.data[0:8])
}

// Revision returns the GPT Header revision number. [0,0,1,0] for UEFI 2.10.
func (h *Header) Revision() uint32 {
	return binary.LittleEndian.Uint32(h.data[8:12])
}

// Size returns the size of the GPT header in bytes, usually 92.
func (h *Header) Size() uint32 {
	return binary.LittleEndian.Uint32(h.data[12:16])
}

// SetSize sets the size of the GPT header in bytes.
func (h *Header) SetSize(size uint32) {
	binary.LittleEndian.PutUint32(h.data[12:16], size)
}

// CRC returns the CRC32 of the GPT header.
func (h *Header) CRC() uint32 {
	return binary.LittleEndian.Uint32(h.data[16:20])
}

// SetCRC sets the CRC32 of the GPT header.
func (h *Header) SetCRC(crc uint32) {
	binary.LittleEndian.PutUint32(h.data[16:20], crc)
}

// Bytes 20..24 are reserved and should be zero.

// CurrentLBA returns the LBA of the current GPT header.
func (h *Header) CurrentLBA() int64 {
	return int64(binary.LittleEndian.Uint64(h.data[24:32]))
}

// SetCurrentLBA sets the LBA of the current GPT header.
func (h *Header) SetCurrentLBA(lba int64) {
	binary.LittleEndian.PutUint64(h.data[24:32], uint64(lba))
}

// BackupLBA returns the LBA of the backup GPT header.
func (h *Header) BackupLBA() int64 {
	return int64(binary.LittleEndian.Uint64(h.data[32:40]))
}

// SetBackupLBA sets the LBA of the backup GPT header.
func (h *Header) SetBackupLBA(lba int64) {
	binary.LittleEndian.PutUint64(h.data[32:40], uint64(lba))
}

// FirstUsableLBA returns the first LBA that is not used by the GPT header, partition table and partition entries.
func (h *Header) FirstUsableLBA() int64 {
	return int64(binary.LittleEndian.Uint64(h.data[40:48]))
}

// SetFirstUsableLBA sets the first LBA that is not used by the GPT header, partition table and partition entries.
func (h *Header) SetFirstUsableLBA(lba int64) {
	binary.LittleEndian.PutUint64(h.data[40:48], uint64(lba))
}

// LastUsableLBA returns the last LBA that is not used by the GPT header, partition table and partition entries.
func (h *Header) LastUsableLBA() int64 {
	return int64(binary.LittleEndian.Uint64(h.data[48:56]))
}

// SetLastUsableLBA sets the last LBA that is not used by the GPT header, partition table and partition entries.
func (h *Header) SetLastUsableLBA(lba int64) {
	binary.LittleEndian.PutUint64(h.data[48:56], uint64(lba))
}

// DiskGUID returns the GUID of the disk.
func (h *Header) DiskGUID() (guid [16]byte) {
	copy(guid[:], h.data[56:72])
	return guid
}

// SetDiskGUID sets the GUID of the disk.
func (h *Header) SetDiskGUID(guid [16]byte) {
	copy(h.data[56:72], guid[:])
}

// PartitionEntryLBA returns the LBA of the start of the partition table.
// This field is usually 2 for compatibility with MBR paritioning.
// This is because 0 is used for the protective MBR and 1 is used for the GPT header.
func (h *Header) PartitionEntryLBA() int64 {
	return int64(binary.LittleEndian.Uint64(h.data[72:80]))
}

// SetPartitionEntryLBA sets the LBA of the start of the partition table.
func (h *Header) SetPartitionEntryLBA(lba int64) {
	binary.LittleEndian.PutUint64(h.data[72:80], uint64(lba))
}

// NumberOfPartitionEntries returns the number of partition entries in the partition table.
func (h *Header) NumberOfPartitionEntries() uint32 {
	return binary.LittleEndian.Uint32(h.data[80:84])
}

// SetNumberOfPartitionEntries sets the number of partition entries in the partition table.
func (h *Header) SetNumberOfPartitionEntries(n uint32) {
	binary.LittleEndian.PutUint32(h.data[80:84], n)
}

// SizeOfPartitionEntry returns the size of each partition entry in the partition table.
// Is usually 128.
func (h *Header) SizeOfPartitionEntry() uint32 {
	return binary.LittleEndian.Uint32(h.data[84:88])
}

// SetSizeOfPartitionEntry sets the size of each partition entry in the partition table.
func (h *Header) SetSizeOfPartitionEntry(size uint32) {
	binary.LittleEndian.PutUint32(h.data[84:88], size)
}

// CRCOfPartitionEntries returns the CRC32 of the partition entries in the partition table.
func (h *Header) CRCOfPartitionEntries() uint32 {
	return binary.LittleEndian.Uint32(h.data[88:92])
}

// SetCRCOfPartitionEntries sets the CRC32 of the partition entries in the partition table.
func (h *Header) SetCRCOfPartitionEntries(crc uint32) {
	binary.LittleEndian.PutUint32(h.data[88:92], crc)
}

// PartitionEntry represents a single partition entry in the GPT partition table. Usually of size 128 bytes.
type PartitionEntry struct {
	data []byte
}

type PartitionAttributes uint64

func ToPartitionEntry(start []byte) (PartitionEntry, error) {
	if len(start) < 128 {
		return PartitionEntry{}, errors.New("gpt partition entry too short")
	}
	p := PartitionEntry{
		data: start[:128:128],
	}
	return p, nil
}

// PartitionTypeGUID returns the GUID of the partition type.
func (p *PartitionEntry) PartitionTypeGUID() (guid [16]byte) {
	copy(guid[:], p.data[0:16])
	return
}

// SetPartitionTypeGUID sets the GUID of the partition type.
func (p *PartitionEntry) SetPartitionTypeGUID(guid [16]byte) {
	copy(p.data[0:16], guid[:])
}

// UniquePartitionGUID returns the GUID of the partition.
func (p *PartitionEntry) UniquePartitionGUID() (guid [16]byte) {
	copy(guid[:], p.data[16:32])
	return
}

// SetUniquePartitionGUID sets the GUID of the partition.
func (p *PartitionEntry) SetUniquePartitionGUID(guid [16]byte) {
	copy(p.data[16:32], guid[:])
}

// FirstLBA returns the first LBA of the partition.
// To calculate total LBAs: (LastLBA - FirstLBA) + 1
func (p *PartitionEntry) FirstLBA() int64 {
	return int64(binary.LittleEndian.Uint64(p.data[32:40]))
}

// SetFirstLBA sets the first LBA of the partition.
func (p *PartitionEntry) SetFirstLBA(lba int64) {
	binary.LittleEndian.PutUint64(p.data[32:40], uint64(lba))
}

// LastLBA returns the last LBA of the partition (inclusive).
// To calculate total LBAs: (LastLBA - FirstLBA) + 1
func (p *PartitionEntry) LastLBA() int64 {
	return int64(binary.LittleEndian.Uint64(p.data[40:48]))
}

// SetLastLBA sets the last LBA of the partition (inclusive).
func (p *PartitionEntry) SetLastLBA(lba int64) {
	binary.LittleEndian.PutUint64(p.data[40:48], uint64(lba))
}

// Attributes returns the attributes of the partition.
func (p *PartitionEntry) Attributes() PartitionAttributes {
	return PartitionAttributes(binary.LittleEndian.Uint64(p.data[48:56]))
}

// SetAttributes sets the attributes of the partition.
func (p *PartitionEntry) SetAttributes(attr PartitionAttributes) {
	binary.LittleEndian.PutUint64(p.data[48:56], uint64(attr))
}

// ReadName reads the partition name from the partition entry and
// encodes it as utf-8 into the provided slice. The number of bytes
// read is returned along with any error.
func (p *PartitionEntry) ReadName(b []byte) (int, error) {
	// Find the length of the name.
	nameLen := 0
	for nameLen < pteNameLen && p.data[pteNameOff+nameLen] != 0 {
		nameLen++
	}

	n, err := utf16x.ToUTF8(b, p.data[pteNameOff:pteNameOff+nameLen], binary.LittleEndian)
	if err != nil {
		return n, err
	}
	return n, nil
}

func (p *PartitionEntry) ClearName() {
	p.data[pteNameOff] = 0
}

// WriteName writes a utf-8 encoded string as the Partition Entry's name.
func (p *PartitionEntry) WriteName(name []byte) error {
	n, err := utf16x.FromUTF8(p.data[pteNameOff:pteNameOff+pteNameLen], name, binary.LittleEndian)
	if err != nil {
		return err
	}

	for i := n; i < pteNameLen; i++ {
		p.data[pteNameOff+i] = 0
	}
	return nil
}
