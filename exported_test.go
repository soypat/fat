package fat

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestSeekReadBack(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	if err := fs.OpenFile(&f, "seektest.dat", ModeCreateAlways|ModeRW); err != nil {
		t.Fatal(err)
	}
	// Write 3 clusters worth of patterned data (cluster size 8*512=4096).
	const size = 3 * 4096
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + 7)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}

	// Seek back to a misaligned offset and read.
	const off = 6789
	n, err := f.Seek(off, io.SeekStart)
	if err != nil || n != off {
		t.Fatalf("Seek: n=%d err=%v", n, err)
	}
	buf := make([]byte, 100)
	if _, err := f.Read(buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[off:off+100]) {
		t.Error("seek+read mismatch at misaligned offset")
	}

	// SeekCurrent and SeekEnd.
	n, err = f.Seek(-50, io.SeekCurrent)
	if err != nil || n != off+100-50 {
		t.Fatalf("SeekCurrent: n=%d err=%v", n, err)
	}
	n, err = f.Seek(-4096, io.SeekEnd)
	if err != nil || n != size-4096 {
		t.Fatalf("SeekEnd: n=%d err=%v", n, err)
	}
	if _, err := f.Read(buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[size-4096:size-4096+100]) {
		t.Error("seek+read mismatch after SeekEnd")
	}

	// Invalid whence and negative position.
	if _, err := f.Seek(0, 42); !errors.Is(err, errWhence) {
		t.Errorf("bad whence: %v", err)
	}
	if _, err := f.Seek(-1, io.SeekStart); !errors.Is(err, errNegativeSeek) {
		t.Errorf("negative seek: %v", err)
	}

	// Seek past EOF in write mode extends the file.
	n, err = f.Seek(size+4096, io.SeekStart)
	if err != nil || n != size+4096 {
		t.Fatalf("seek past EOF: n=%d err=%v", n, err)
	}
	if _, err := f.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	var finfo FileInfo
	found := false
	var dir Dir
	if err := fs.OpenDir(&dir, "/"); err != nil {
		t.Fatal(err)
	}
	err = dir.ForEachFile(func(fi *FileInfo) error {
		if fi.Name() == "seektest.dat" {
			finfo = *fi
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found || finfo.Size() != size+4096+4 {
		t.Fatalf("extended size = %d, want %d (found=%v)", finfo.Size(), size+4096+4, found)
	}
}

func TestSeekOverwriteMidFile(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	if err := fs.OpenFile(&f, "mid.dat", ModeCreateAlways|ModeRW); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 2*4096)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	// Overwrite 1000 bytes at misaligned offset 777.
	if _, err := f.Seek(777, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	patch := bytes.Repeat([]byte{0xAB}, 1000)
	if _, err := f.Write(patch); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	copy(data[777:], patch)

	if err := fs.OpenFile(&f, "mid.dat", ModeRead); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if _, err := io.ReadFull(&f, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("mid-file overwrite content mismatch")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemove(t *testing.T) {
	fs, _ := initTestFAT()

	// Remove nonexistent file.
	if err := fs.Remove("nonexistent"); err == nil {
		t.Error("expected error removing nonexistent file")
	}

	// Create a multi-cluster file, remove it, expect gone.
	var f File
	if err := fs.OpenFile(&f, "doomed.dat", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 3*4096)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove("doomed.dat"); err != nil {
		t.Fatal(err)
	}
	if err := fs.OpenFile(&f, "doomed.dat", ModeRead); err == nil {
		t.Error("file still openable after Remove")
	}

	// Remove file with LFN entries.
	if err := fs.OpenFile(&f, "long file name removal test.txt", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("bye")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove("long file name removal test.txt"); err != nil {
		t.Fatal(err)
	}
	var dir Dir
	if err := fs.OpenDir(&dir, "/"); err != nil {
		t.Fatal(err)
	}
	err := dir.ForEachFile(func(fi *FileInfo) error {
		if fi.Name() == "long file name removal test.txt" {
			t.Error("LFN file still listed after Remove")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Non-empty directory must be denied.
	if err := fs.Remove("rootdir"); err == nil {
		t.Error("expected denied removing non-empty directory")
	}
	// Removing a file inside a directory works.
	if err := fs.Remove("rootdir/dirfile"); err != nil {
		t.Fatal(err)
	}
	// Now the directory is empty and removable.
	if err := fs.Remove("rootdir"); err != nil {
		t.Fatal(err)
	}
	if err := fs.OpenDir(&dir, "rootdir"); err == nil {
		t.Error("directory still openable after Remove")
	}
}

// TestRemoveReallocate punches a FAT hole with Remove then verifies a new
// larger file threads through the freed clusters without corrupting a
// neighboring file.
func TestRemoveReallocate(t *testing.T) {
	fs, _ := initTestFAT()
	pat := func(tag, n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i*31 + tag*17 + 7)
		}
		return b
	}
	write := func(name string, data []byte) {
		t.Helper()
		var f File
		if err := fs.OpenFile(&f, name, ModeCreateAlways|ModeWrite); err != nil {
			t.Fatal(name, err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatal(name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(name, err)
		}
	}
	readAll := func(name string) []byte {
		t.Helper()
		var f File
		if err := fs.OpenFile(&f, name, ModeRead); err != nil {
			t.Fatal(name, err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, &f); err != nil {
			t.Fatal(name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(name, err)
		}
		return buf.Bytes()
	}

	a := pat(1, 2*4096)
	b := pat(2, 2*4096)
	c := pat(3, 4*4096)
	write("hole1.dat", a)
	write("keep.dat", b)
	write("hole2.dat", a)
	if err := fs.Remove("hole1.dat"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove("hole2.dat"); err != nil {
		t.Fatal(err)
	}
	// New file larger than either hole: chain must fragment around keep.dat.
	write("frag.dat", c)
	if got := readAll("frag.dat"); !bytes.Equal(got, c) {
		t.Error("fragmented file content mismatch")
	}
	if got := readAll("keep.dat"); !bytes.Equal(got, b) {
		t.Error("neighbor file corrupted by reallocation")
	}
}
