package fat

import (
	"testing"
	"time"
)

func bootSectorCopy() []byte {
	sect := fatInit[0]
	buf := make([]byte, 512)
	copy(buf, sect[:])
	return buf
}

func TestBiosParamBlockAccessors(t *testing.T) {
	bs := biosParamBlock{data: bootSectorCopy()}

	// Known values from the fatInit FAT32 boot sector.
	if got := bs.SectorSize(); got != 512 {
		t.Errorf("SectorSize = %d", got)
	}
	if got := bs.SectorsPerCluster(); got != 8 {
		t.Errorf("SectorsPerCluster = %d", got)
	}
	if got := bs.NumberOfFATs(); got != 2 {
		t.Errorf("NumberOfFATs = %d", got)
	}
	if got := bs.ReservedSectors(); got != 32 {
		t.Errorf("ReservedSectors = %d", got)
	}
	if got := bs.RootDirEntries(); got != 0 {
		t.Errorf("RootDirEntries = %d", got)
	}
	if got := bs.RootCluster(); got != 2 {
		t.Errorf("RootCluster = %d", got)
	}
	if got := bs.BootSignature(); got != 0xAA55 {
		t.Errorf("BootSignature = %#x", got)
	}
	if got := bs.VolumeLabel(); string(got[:]) != "keylargo   " {
		t.Errorf("VolumeLabel = %q", got)
	}
	if got := bs.OEMName(); string(got[:]) != "mkfs.fat" {
		t.Errorf("OEMName = %q", got)
	}
	if got := bs.FilesystemType(); string(got[:]) != "FAT32   " {
		t.Errorf("FilesystemType = %q", got)
	}
	jmp := bs.JumpInstruction()
	if jmp[0] != 0xEB {
		t.Errorf("JumpInstruction = %#x", jmp)
	}
	if major, minor := bs.Version(); major != 0 || minor != 0 {
		t.Errorf("Version = %d.%d", major, minor)
	}
	if got := bs.FSInfo(); got != 1 {
		t.Errorf("FSInfo = %d", got)
	}
	if got := bs.ExtendedBootSignature(); got != 0x29 {
		t.Errorf("ExtendedBootSignature = %#x", got)
	}
	_ = bs.DriveNumber()
	_ = bs.VolumeSerialNumber()
	_ = bs.VolumeOffset()
	_ = bs.TotalSectors()
	_ = bs.SectorsPerFAT()
	if s := bs.String(); len(s) == 0 {
		t.Error("empty String()")
	}

	// Setter round-trips.
	bs.SetSectorSize(4096)
	if got := bs.SectorSize(); got != 4096 {
		t.Errorf("SetSectorSize round-trip = %d", got)
	}
	bs.SetSectorsPerCluster(16)
	if got := bs.SectorsPerCluster(); got != 16 {
		t.Errorf("SetSectorsPerCluster round-trip = %d", got)
	}
	bs.SetNumberOfFATs(1)
	if got := bs.NumberOfFATs(); got != 1 {
		t.Errorf("SetNumberOfFATs round-trip = %d", got)
	}
	bs.SetReservedSectors(64)
	if got := bs.ReservedSectors(); got != 64 {
		t.Errorf("SetReservedSectors round-trip = %d", got)
	}
	bs.SetRootDirEntries(512)
	if got := bs.RootDirEntries(); got != 512 {
		t.Errorf("SetRootDirEntries round-trip = %d", got)
	}
	bs.SetRootCluster(7)
	if got := bs.RootCluster(); got != 7 {
		t.Errorf("SetRootCluster round-trip = %d", got)
	}
	bs.SetSectorsPerFAT(1234)
	if got := bs.SectorsPerFAT(); got != 1234 {
		t.Errorf("SetSectorsPerFAT round-trip = %d", got)
	}
	bs.SetTotalSectors(99999)
	if got := bs.TotalSectors(); got != 99999 {
		t.Errorf("SetTotalSectors round-trip = %d", got)
	}
	bs.SetVolumeLabel("HELLO")
	lbl := bs.VolumeLabel()
	if string(lbl[:5]) != "HELLO" {
		t.Errorf("SetVolumeLabel round-trip = %q", lbl)
	}
	bs.SetOEMName("gofatlib")
	oem := bs.OEMName()
	if string(oem[:]) != "gofatlib" {
		t.Errorf("SetOEMName round-trip = %q", oem)
	}
}

func TestFSInfoSectorAccessors(t *testing.T) {
	sect := fatInit[1]
	buf := make([]byte, 512)
	copy(buf, sect[:])
	fsi := fsinfoSector{data: buf}

	s1, s2, s3 := fsi.Signatures()
	if s1 != 0x41615252 || s2 != 0x61417272 || s3 != 0xAA550000 {
		t.Errorf("Signatures = %#x %#x %#x", s1, s2, s3)
	}
	if got := fsi.FreeClusterCount(); got != 0x001df1f8 {
		t.Errorf("FreeClusterCount = %#x", got)
	}
	if got := fsi.LastAllocatedCluster(); got != 5 {
		t.Errorf("LastAllocatedCluster = %d", got)
	}
	fsi.SetFreeClusterCount(42)
	if got := fsi.FreeClusterCount(); got != 42 {
		t.Errorf("SetFreeClusterCount round-trip = %d", got)
	}
	fsi.SetLastAllocatedCluster(77)
	if got := fsi.LastAllocatedCluster(); got != 77 {
		t.Errorf("SetLastAllocatedCluster round-trip = %d", got)
	}
	fsi.SetSignatures(0x41615252, 0x61417272, 0xAA550000)
	s1, s2, s3 = fsi.Signatures()
	if s1 != 0x41615252 || s2 != 0x61417272 || s3 != 0xAA550000 {
		t.Errorf("SetSignatures round-trip = %#x %#x %#x", s1, s2, s3)
	}
	if s := fsi.String(); len(s) == 0 {
		t.Error("empty String()")
	}
}

func TestFAT32SectorEntries(t *testing.T) {
	sect := fatInit[32]
	buf := make([]byte, 512)
	copy(buf, sect[:])
	fs := fat32Sector{data: buf}

	if e := fs.Entry(1); !e.IsEOF() {
		t.Errorf("entry 1 = %#x, want EOF", uint32(e))
	}
	if e := fs.Entry(3); e.Cluster() != 0x0FFFFFFF {
		t.Errorf("entry 3 cluster = %#x", e.Cluster())
	}
	fs.SetEntry(9, 0x00000010)
	if e := fs.Entry(9); e.Cluster() != 0x10 || e.IsEOF() {
		t.Errorf("SetEntry round-trip = %#x", uint32(e))
	}
	if s := fs.String(); len(s) == 0 {
		t.Error("empty String()")
	}
}

func TestDatetimeRoundTrip(t *testing.T) {
	ref := time.Date(2024, time.March, 15, 13, 37, 21, 0, time.UTC)
	dt := newDatetime(ref)
	if got := dt.Time(); !got.Equal(ref) {
		t.Errorf("Time round-trip = %v, want %v", got, ref)
	}
	y, m, d := dt.Date()
	if y != 2024 || m != time.March || d != 15 {
		t.Errorf("Date = %d-%v-%d", y, m, d)
	}
	h, min, sec := dt.Clock()
	if h != 13 || min != 37 || sec != 21 {
		t.Errorf("Clock = %d:%d:%d", h, min, sec)
	}
	// Odd second is stored in the fine (10ms resolution) field.
	if ms := dt.Milliseconds(); ms != 0 {
		t.Errorf("Milliseconds = %d", ms)
	}
	even := newDatetime(time.Date(2000, time.January, 2, 3, 4, 6, 130e6, time.UTC))
	if ms := even.Milliseconds(); ms != 130 {
		t.Errorf("Milliseconds = %d, want 130", ms)
	}
}

// TestDirSectorEntries exercises the raw directory entry accessors over the
// known fatInit root directory sector.
func TestDirSectorEntries(t *testing.T) {
	sect := fatInit[30704]
	buf := make([]byte, 512)
	copy(buf, sect[:])

	// Entry 0: volume label "keylargo".
	vol := dirSector{data: buf[0:32]}
	if vol.isFree() || vol.isDeleted() || vol.isDotEntry() {
		t.Error("volume label misclassified")
	}
	if !vol.attributes().IsVolumeLabel() {
		t.Error("expected volume label attribute")
	}
	name := vol.shortfilename()
	if string(name[:]) != "keylargo" {
		t.Errorf("shortfilename = %q", name)
	}

	// Entry 1: LFN entry for "rootfile".
	lfn := longFilenameEntry{data: buf[32:64]}
	if !fileattr(lfn.Attributes()).IsLFN() {
		t.Error("expected LFN attribute")
	}
	seq := lfn.Sequence()
	if !seq.IsLast() || seq.SequenceNumber() != 1 || seq.IsDeleted() {
		t.Errorf("LFN seq = %#x", byte(seq))
	}
	if lfn.FirstCluster() != 0 {
		t.Errorf("LFN FirstCluster = %d", lfn.FirstCluster())
	}
	if lfn.Type() != 0 {
		t.Errorf("LFN Type = %d", lfn.Type())
	}
	if lfn.Checksum() == 0 {
		t.Error("LFN checksum zero")
	}
	var nameData [26]byte
	lfn.ReadData(nameData[:])
	if nameData[0] != 'r' || nameData[2] != 'o' {
		t.Errorf("LFN data = %q", nameData)
	}

	// Entry 2: SFN "ROOTFILE".
	sfn := dirSector{data: buf[64:96]}
	attr := sfn.attributes()
	if attr.IsLFN() || attr.IsSubdirectory() || attr.IsReadonly() ||
		attr.IsHidden() || attr.IsSystem() || attr.IsDevice() || attr.IsVolumeLabel() {
		t.Errorf("SFN attributes = %#x", byte(attr))
	}
	name = sfn.shortfilename()
	if string(name[:]) != "ROOTFILE" {
		t.Errorf("shortfilename = %q", name)
	}
	ext := sfn.shortfilext()
	if string(ext[:]) != "   " {
		t.Errorf("shortfilext = %q", ext)
	}
	if sfn.cluster() != 4 {
		t.Errorf("cluster = %d", sfn.cluster())
	}
	if sfn.size() != 22 {
		t.Errorf("size = %d", sfn.size())
	}
	if y, _, _ := sfn.createdAt().Date(); y < 2000 {
		t.Errorf("createdAt year = %d", y)
	}
	if y, _, _ := sfn.modifiedAt().Date(); y < 2000 {
		t.Errorf("modifiedAt year = %d", y)
	}
	_ = sfn.accessedAt()

	// Entry 5 (offset 160): deleted LFN entry.
	del := dirSector{data: buf[160:192]}
	if !del.isDeleted() {
		t.Error("expected deleted entry")
	}
	delseq := longFilenameEntry{data: buf[160:192]}
	if !delseq.Sequence().IsDeleted() {
		t.Error("expected deleted LFN sequence")
	}

	// ROOTDIR entry at offset 128 is a subdirectory.
	dir := dirSector{data: buf[128:160]}
	if !dir.attributes().IsSubdirectory() {
		t.Error("expected subdirectory attribute")
	}
	if !dir.attributes().IsArchive() == false && dir.attributes().IsArchive() {
		t.Error("unexpected archive attribute")
	}
}
