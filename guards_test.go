package fat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestMountGuards(t *testing.T) {
	var fsys FS
	blk, _ := makeBlockIndexer(512)
	dev := &BlockByteSlice{blk: blk, buf: make([]byte, 64*512)}

	// Invalid permission bits.
	if err := fsys.Mount(dev, 512, Mode(0b100)); err != errInvalidMode {
		t.Errorf("invalid mode: %v", err)
	}
	// Sector size too large for uint16.
	if err := fsys.Mount(dev, 1<<17, ModeRW); err == nil {
		t.Error("expected error for huge sector size")
	}
	// Non power-of-two sector size.
	if err := fsys.Mount(dev, 1000, ModeRW); err == nil {
		t.Error("expected error for non power-of-two sector size")
	}
	// Zeroed device has no filesystem.
	empty := &BlockByteSlice{buf: make([]byte, 64*512)}
	empty.blk, _ = makeBlockIndexer(512)
	if err := fsys.Mount(empty, 512, ModeRW); err == nil {
		t.Error("expected error mounting zeroed device")
	}
}

func TestOpenFileGuards(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	// Invalid mode bits.
	if err := fs.OpenFile(&f, "rootfile", Mode(0x40)); err != errInvalidMode {
		t.Errorf("invalid mode: %v", err)
	}
	// Nonexistent file.
	if err := fs.OpenFile(&f, "nope.txt", ModeRead); err == nil {
		t.Error("expected error opening nonexistent file")
	}
	// Opening a directory as a file fails.
	if err := fs.OpenFile(&f, "rootdir", ModeRead); err == nil {
		t.Error("expected error opening directory as file")
	}
	// CreateNew on existing file fails.
	if err := fs.OpenFile(&f, "rootfile", ModeCreateNew|ModeWrite); err == nil {
		t.Error("expected error for CreateNew on existing file")
	}
	// Write on file opened read-only is rejected.
	if err := fs.OpenFile(&f, "rootfile", ModeRead); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); err == nil {
		t.Error("expected error writing to read-only file handle")
	}
	// Read on file opened write-only is rejected.
	var g File
	if err := fs.OpenFile(&g, "rootfile", ModeWrite); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := g.Read(buf); err == nil {
		t.Error("expected error reading write-only file handle")
	}
}

func TestReadOnlyMount(t *testing.T) {
	dev := goldenDevice(t, "golden-torture16.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatal(err)
	}
	var f File
	if err := fsys.OpenFile(&f, "cc.dat", ModeWrite); err != errForbiddenMode {
		t.Errorf("write open on RO mount: %v", err)
	}
	if err := fsys.Remove("cc.dat"); err == nil {
		t.Error("expected error removing on RO mount")
	}
	if err := fsys.OpenFile(&f, "cc.dat", ModeRead); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := f.Read(buf); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClosedFileOps(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	if err := fs.OpenFile(&f, "rootfile", ModeRW); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := f.Read(buf); err == nil {
		t.Error("read on closed file")
	}
	if _, err := f.Write(buf); err == nil {
		t.Error("write on closed file")
	}
	if _, err := f.Seek(0, io.SeekStart); err == nil {
		t.Error("seek on closed file")
	}
	if err := f.Sync(); err == nil {
		t.Error("sync on closed file")
	}
	if err := f.Close(); err == nil {
		t.Error("double close")
	}
}

func TestReadAtEOF(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	if err := fs.OpenFile(&f, "rootfile", ModeRead); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	n, err := f.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("read at EOF: n=%d err=%v", n, err)
	}
}

// TestMBRPartition builds an MBR-partitioned image with the FAT16 golden
// volume at partition 1 and verifies find_volume's partition scan.
func TestMBRPartition(t *testing.T) {
	const partStart = 2048
	vol := goldenImage(t, "golden-fmt16.img")
	buf := make([]byte, partStart*512+len(vol))
	copy(buf[partStart*512:], vol)
	// MBR: partition table entry 0 at offset 446. Byte 4: type (0x0E,
	// FAT16 LBA), bytes 8-11: start LBA, bytes 12-15: sector count.
	buf[446+4] = 0x0E
	binary.LittleEndian.PutUint32(buf[446+8:], partStart)
	binary.LittleEndian.PutUint32(buf[446+12:], uint32(len(vol)/512))
	buf[510] = 0x55
	buf[511] = 0xAA

	blk, _ := makeBlockIndexer(512)
	dev := &BlockByteSlice{blk: blk, buf: buf}
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("mount MBR-partitioned volume: %v", err)
	}
	if fsys.fstype != fstypeFAT16 {
		t.Fatalf("fstype = %d, want FAT16", fsys.fstype)
	}
	// The volume is usable: create and read back a file.
	writeStr(t, &fsys, "part.txt", "partitioned")
	if got := readAllFile(t, &fsys, "part.txt"); string(got) != "partitioned" {
		t.Errorf("part.txt = %q", got)
	}
}

// TestGPTUnsupported verifies a GPT protective MBR is rejected (the port has
// no GPT support yet).
func TestGPTUnsupported(t *testing.T) {
	buf := make([]byte, 64*512)
	buf[446+4] = 0xEE // GPT protective partition type.
	buf[510] = 0x55
	buf[511] = 0xAA
	blk, _ := makeBlockIndexer(512)
	dev := &BlockByteSlice{blk: blk, buf: buf}
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err == nil {
		t.Fatal("expected error mounting GPT device")
	}
}

// TestExFATUnsupported verifies an exFAT VBR is recognized and rejected.
func TestExFATUnsupported(t *testing.T) {
	buf := make([]byte, 64*512)
	copy(buf, "\xEB\x76\x90EXFAT   ")
	buf[510] = 0x55
	buf[511] = 0xAA
	blk, _ := makeBlockIndexer(512)
	dev := &BlockByteSlice{blk: blk, buf: buf}
	var fsys FS
	err := fsys.Mount(dev, 512, ModeRW)
	if !errors.Is(err, frUnsupported) {
		t.Fatalf("exFAT mount error = %v, want unsupported", err)
	}
}

// TestFastSeekCLMT exercises the FF_USE_FASTSEEK-equivalent cluster link map
// table: build a fragmented file, create its CLMT and seek through it.
func TestFastSeekCLMT(t *testing.T) {
	fs, _ := initTestFAT()
	// Fragment: a.dat, hole.dat, extend a.dat, remove hole.dat, extend more.
	createPat(t, fs, "a.dat", 1, 2*4096)
	createPat(t, fs, "hole.dat", 2, 2*4096)
	appendPat(t, fs, "a.dat", 1, 2*4096, 2*4096)
	if err := fs.Remove("hole.dat"); err != nil {
		t.Fatal(err)
	}

	var f File
	if err := fs.OpenFile(&f, "a.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatal(err)
	}
	// Too-small table reports not enough memory.
	f.cltbl = make([]uint32, 2)
	f.cltbl[0] = 2
	if fr := f.f_lseek(createLinkmap); fr != frNotEnoughCore {
		t.Fatalf("tiny CLMT = %v, want frNotEnoughCore", fr)
	}
	// Adequate table.
	f.cltbl = make([]uint32, 16)
	f.cltbl[0] = 16
	if fr := f.f_lseek(createLinkmap); fr != frOK {
		t.Fatalf("create CLMT: %v", fr)
	}
	// Fast seek to a misaligned offset and verify content.
	if fr := f.f_lseek(4096 + 1234); fr != frOK {
		t.Fatalf("fast seek: %v", fr)
	}
	buf := make([]byte, 64)
	if _, err := f.Read(buf); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 64)
	for i := range want {
		want[i] = pat(1, 4096+1234+i)
	}
	if !bytes.Equal(buf, want) {
		t.Error("fast-seek read content mismatch")
	}
	// Fast seek clips at file size.
	if fr := f.f_lseek(1 << 30); fr != frOK {
		t.Fatalf("fast seek clip: %v", fr)
	}
	if f.fptr != f.obj.objsize {
		t.Errorf("clipped fptr = %d, want %d", f.fptr, f.obj.objsize)
	}
	f.cltbl = nil
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// failingDevice wraps a BlockDevice and fails all operations once tripped.
type failingDevice struct {
	dev       BlockDeviceExtended
	failWr    bool
	failRd    bool
	errInject error
}

func (fd *failingDevice) ReadBlocks(dst []byte, start int64) (int, error) {
	if fd.failRd {
		return 0, fd.errInject
	}
	return fd.dev.ReadBlocks(dst, start)
}

func (fd *failingDevice) WriteBlocks(data []byte, start int64) (int, error) {
	if fd.failWr {
		return 0, fd.errInject
	}
	return fd.dev.WriteBlocks(data, start)
}

func (fd *failingDevice) EraseBlocks(start, num int64) error {
	if fd.failWr {
		return fd.errInject
	}
	return fd.dev.EraseBlocks(start, num)
}

func TestDiskErrorPaths(t *testing.T) {
	errBoom := errors.New("boom")
	newFS := func() (*FS, *failingDevice) {
		t.Helper()
		dev := DefaultFATByteBlocks(32000)
		fd := &failingDevice{dev: dev, errInject: errBoom}
		var fsys FS
		if err := fsys.Mount(fd, 512, ModeRW); err != nil {
			t.Fatal(err)
		}
		return &fsys, fd
	}

	// Read error aborts the file: subsequent operations return the sticky
	// error until reopened.
	fs, fd := newFS()
	var f File
	if err := fs.OpenFile(&f, "rootfile", ModeRW); err != nil {
		t.Fatal(err)
	}
	fd.failRd = true
	buf := make([]byte, 32)
	if _, err := f.Read(buf); err == nil {
		t.Error("expected read error")
	}
	fd.failRd = false
	if _, err := f.Write(buf); err == nil {
		t.Error("expected sticky abort error on write after failed read")
	}

	// Write error during multi-cluster write.
	fs, fd = newFS()
	if err := fs.OpenFile(&f, "big.dat", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatal(err)
	}
	fd.failWr = true
	if _, err := f.Write(make([]byte, 3*4096)); err == nil {
		t.Error("expected write error")
	}
	fd.failWr = false

	// Mount with failing reads reports a disk error.
	blk, _ := makeBlockIndexer(512)
	dev := &BlockByteSlice{blk: blk, buf: make([]byte, 64*512)}
	fdm := &failingDevice{dev: dev, errInject: errBoom, failRd: true}
	var fsys FS
	if err := fsys.Mount(fdm, 512, ModeRW); err == nil {
		t.Error("expected mount error on failing device")
	}
}

func TestFileInfoAccessors(t *testing.T) {
	fs, _ := initTestFAT()
	var dir Dir
	if err := fs.OpenDir(&dir, "/"); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	err := dir.ForEachFile(func(fi *FileInfo) error {
		// Lowercase names: the fat_nolfn build reports the uppercase SFN.
		name := strings.ToLower(fi.Name())
		seen[name] = true
		switch name {
		case "rootfile":
			if fi.IsDir() {
				t.Error("rootfile reported as directory")
			}
			if fi.Size() != int64(len(rootFileContents)) {
				t.Errorf("rootfile size = %d", fi.Size())
			}
			if lfnEnabled && fi.AlternateName() != "ROOTFILE" {
				t.Errorf("altname = %q", fi.AlternateName())
			}
			if y := fi.ModTime().Year(); y < 2000 {
				t.Errorf("mod time year = %d", y)
			}
		case "rootdir":
			if !fi.IsDir() {
				t.Error("rootdir not reported as directory")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !seen["rootfile"] || !seen["rootdir"] {
		t.Errorf("missing entries: %v", seen)
	}
	// Callback errors propagate.
	sentinel := errors.New("stop")
	if err := dir.ForEachFile(func(*FileInfo) error { return sentinel }); err != sentinel {
		t.Errorf("callback error = %v", err)
	}
	// Opening a file as directory fails.
	if err := fs.OpenDir(&dir, "rootfile"); err == nil {
		t.Error("expected error opening file as directory")
	}
}
