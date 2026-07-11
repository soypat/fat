package fat

import (
	"bytes"
	"errors"
	"io"
	"strings"
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
		// Case-insensitive: the fat_nolfn build reports the uppercase SFN.
		if strings.EqualFold(fi.Name(), "seektest.dat") {
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

func TestReadAtWriteAt(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	if err := fs.OpenFile(&f, "at.dat", ModeCreateAlways|ModeRW); err != nil {
		t.Fatal(err)
	}
	// Two clusters of patterned data (cluster size 8*512=4096).
	const size = 2 * 4096
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*13 + 5)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}

	// Position offset mid-file; ReadAt/WriteAt must not disturb it.
	const cur = 1000
	if _, err := f.Seek(cur, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	// ReadAt at a misaligned cross-cluster offset.
	buf := make([]byte, 300)
	n, err := f.ReadAt(buf, 4000)
	if err != nil || n != len(buf) {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf, data[4000:4300]) {
		t.Error("ReadAt content mismatch")
	}
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != cur {
		t.Fatalf("ReadAt moved offset to %d, want %d", pos, cur)
	}

	// ReadAt clipped at EOF returns io.EOF.
	n, err = f.ReadAt(buf, size-100)
	if n != 100 || err != io.EOF {
		t.Fatalf("ReadAt at EOF: n=%d err=%v", n, err)
	}
	// ReadAt past EOF must not extend the file even in write mode.
	if n, err = f.ReadAt(buf, size+5000); n != 0 || err != io.EOF {
		t.Fatalf("ReadAt past EOF: n=%d err=%v", n, err)
	}
	if f.Size() != size {
		t.Fatalf("ReadAt past EOF grew file to %d", f.Size())
	}
	if _, err = f.ReadAt(buf, -1); err != errNegativeOffset {
		t.Fatalf("ReadAt negative offset: %v", err)
	}

	// WriteAt overwrite mid-file preserves offset.
	repl := []byte("HELLOWORLD")
	n, err = f.WriteAt(repl, 4090) // Straddles cluster boundary.
	if err != nil || n != len(repl) {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != cur {
		t.Fatalf("WriteAt moved offset to %d, want %d", pos, cur)
	}
	copy(data[4090:], repl)
	if _, err = f.ReadAt(buf, 4000); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[4000:4300]) {
		t.Error("WriteAt content mismatch")
	}

	// WriteAt past EOF extends the file.
	if _, err = f.WriteAt([]byte("tail"), size+100); err != nil {
		t.Fatal(err)
	}
	if f.Size() != size+104 {
		t.Fatalf("WriteAt past EOF: size=%d want %d", f.Size(), size+104)
	}
	if _, err = f.WriteAt(repl, -1); err != errNegativeOffset {
		t.Fatalf("WriteAt negative offset: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// WriteAt on read-only handle fails; ReadAt still works and preserves offset.
	if err := fs.OpenFile(&f, "at.dat", ModeRead); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(repl, 0); err == nil {
		t.Fatal("WriteAt on read-only file succeeded")
	}
	if _, err = f.ReadAt(buf, 4000); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[4000:4300]) {
		t.Error("read-only ReadAt content mismatch")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteString(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	if err := fs.OpenFile(&f, "ws.dat", ModeCreateAlways|ModeRW); err != nil {
		t.Fatal(err)
	}
	const s = "written by WriteString"
	n, err := f.WriteString(s)
	if err != nil || n != len(s) {
		t.Fatalf("WriteString: n=%d err=%v", n, err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(s))
	if _, err := io.ReadFull(&f, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != s {
		t.Errorf("read back %q, want %q", buf, s)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTruncate(t *testing.T) {
	fs, _ := initTestFAT()
	var f File
	if err := fs.OpenFile(&f, "trunc.dat", ModeCreateAlways|ModeRW); err != nil {
		t.Fatal(err)
	}
	// Three clusters of patterned data.
	const size = 3 * 4096
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if f.Size() != size {
		t.Fatalf("Size=%d want %d", f.Size(), size)
	}

	// Shrink to a misaligned mid-file size; offset (at EOF) clamps to new size.
	const shrunk = 4096 + 100
	if err := f.Truncate(shrunk); err != nil {
		t.Fatal(err)
	}
	if f.Size() != shrunk {
		t.Fatalf("after shrink Size=%d want %d", f.Size(), shrunk)
	}
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != shrunk {
		t.Fatalf("offset after shrink = %d, want %d", pos, shrunk)
	}
	// Data before the truncation point intact.
	buf := make([]byte, shrunk)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[:shrunk]) {
		t.Error("content mismatch after shrink")
	}

	// Truncate with offset before the cut leaves offset alone.
	if _, err := f.Seek(50, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1000); err != nil {
		t.Fatal(err)
	}
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != 50 {
		t.Fatalf("offset after shrink = %d, want 50", pos)
	}

	// Grow: size changes, gap writable, offset untouched.
	if err := f.Truncate(2 * 4096); err != nil {
		t.Fatal(err)
	}
	if f.Size() != 2*4096 {
		t.Fatalf("after grow Size=%d want %d", f.Size(), 2*4096)
	}
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != 50 {
		t.Fatalf("offset after grow = %d, want 50", pos)
	}
	if _, err := f.WriteAt([]byte("gap"), 2*4096-3); err != nil {
		t.Fatal(err)
	}

	// Shrink to zero removes the whole chain.
	if err := f.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if f.Size() != 0 {
		t.Fatalf("after zero-truncate Size=%d", f.Size())
	}
	if err := f.Truncate(-1); err != errNegativeOffset {
		t.Fatalf("negative truncate: %v", err)
	}
	// Write after zero-truncate reallocates a fresh chain.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("fresh"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Truncated size persisted to directory entry; read-only Truncate fails.
	if err := fs.OpenFile(&f, "trunc.dat", ModeRead); err != nil {
		t.Fatal(err)
	}
	if f.Size() != 5 {
		t.Fatalf("persisted Size=%d want 5", f.Size())
	}
	if err := f.Truncate(0); err == nil {
		t.Fatal("Truncate on read-only file succeeded")
	}
	buf = make([]byte, 5)
	if _, err := f.ReadAt(buf, 0); err != nil || string(buf) != "fresh" {
		t.Fatalf("read back %q err=%v", buf, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMkdirStatRename(t *testing.T) {
	fs, _ := initTestFAT()

	// Mkdir in root, then nested.
	if err := fs.Mkdir("newdir"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Mkdir("newdir"); err == nil {
		t.Error("expected error creating existing directory")
	}
	if err := fs.Mkdir("newdir/nested"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Mkdir("no/such/parent"); err == nil {
		t.Error("expected error creating directory with missing parent")
	}

	// Stat directory.
	var info FileInfo
	if err := fs.Stat("newdir", &info); err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Error("Stat: newdir not reported as directory")
	}

	// Create a file inside the nested directory and stat it.
	var f File
	if err := fs.OpenFile(&f, "newdir/nested/hello.txt", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("hello world"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Stat("newdir/nested/hello.txt", &info); err != nil {
		t.Fatal(err)
	}
	if info.IsDir() || info.Size() != 11 {
		t.Errorf("Stat: IsDir=%v Size=%d, want file of size 11", info.IsDir(), info.Size())
	}
	if err := fs.Stat("nonexistent", &info); err == nil {
		t.Error("expected error statting nonexistent file")
	}

	readBack := func(path, want string) {
		t.Helper()
		var rf File
		if err := fs.OpenFile(&rf, path, ModeRead); err != nil {
			t.Fatalf("open %q: %v", path, err)
		}
		buf := make([]byte, len(want))
		if _, err := rf.ReadAt(buf, 0); err != nil || string(buf) != want {
			t.Fatalf("read %q: got %q err=%v", path, buf, err)
		}
		rf.Close()
	}

	// Rename within same directory.
	if err := fs.Rename("newdir/nested/hello.txt", "newdir/nested/bye.txt"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Stat("newdir/nested/hello.txt", &info); err == nil {
		t.Error("old name still exists after Rename")
	}
	readBack("newdir/nested/bye.txt", "hello world")

	// Move file across directories.
	if err := fs.Rename("newdir/nested/bye.txt", "moved.txt"); err != nil {
		t.Fatal(err)
	}
	readBack("moved.txt", "hello world")

	// Rename to existing name must fail.
	if err := fs.Rename("moved.txt", "rootfile"); err == nil {
		t.Error("expected error renaming to existing name")
	}
	if err := fs.Rename("nonexistent", "whatever"); err == nil {
		t.Error("expected error renaming nonexistent file")
	}

	// Move a directory into another directory: ".." entry must be updated
	// so paths through the moved directory keep working.
	if err := fs.OpenFile(&f, "newdir/nested/inner.txt", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("inner"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Rename("newdir/nested", "nested"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Stat("newdir/nested", &info); err == nil {
		t.Error("directory still at old path after Rename")
	}
	readBack("nested/inner.txt", "inner")
	if err := fs.Mkdir("nested/sub"); err != nil {
		t.Fatal(err)
	}
}

func TestUnmount(t *testing.T) {
	fs, dev := initTestFAT()

	var f File
	if err := fs.OpenFile(&f, "um.txt", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Unmount(); err != nil {
		t.Fatal(err)
	}

	// Operations after unmount must fail.
	if err := fs.OpenFile(&f, "um.txt", ModeRead); err == nil {
		t.Error("OpenFile succeeded after Unmount")
	}
	if err := fs.Mkdir("x"); err == nil {
		t.Error("Mkdir succeeded after Unmount")
	}
	if err := fs.Stat("um.txt", nil); err == nil {
		t.Error("Stat succeeded after Unmount")
	}
	if err := fs.Unmount(); err == nil {
		t.Error("double Unmount succeeded")
	}

	// Remount: data persisted.
	if err := fs.Mount(dev, int(dev.BlockSize()), ModeRW); err != nil {
		t.Fatal(err)
	}
	var info FileInfo
	if err := fs.Stat("um.txt", &info); err != nil {
		t.Fatal(err)
	}
	if info.Size() != 4 {
		t.Errorf("Size=%d after remount, want 4", info.Size())
	}
}

// TestFileInfoName verifies Name returns exactly the entry's name and not
// the rest of the fixed-size name buffer: no NUL terminator, no leftover
// bytes when a FileInfo is reused across entries of decreasing name length.
func TestFileInfoName(t *testing.T) {
	fs, _ := initTestFAT()
	names := []string{"a long file name test.txt", "ab.txt"}
	if !lfnEnabled {
		names = []string{"longname.txt", "ab.txt"} // 8.3 names only.
	}
	for _, name := range names {
		var f File
		if err := fs.OpenFile(&f, "/"+name, ModeCreateNew|ModeWrite); err != nil {
			t.Fatal(name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(name, err)
		}
	}
	seen := map[string]bool{"rootfile": false, "rootdir": false}
	for _, name := range names {
		seen[name] = false
	}
	var dir Dir
	if err := fs.OpenDir(&dir, "/"); err != nil {
		t.Fatal(err)
	}
	var fi FileInfo
	for {
		// Poison the reused buffer: a correct Name may not depend on it
		// being zeroed beyond the entry's own terminator.
		for i := range fi.fname {
			fi.fname[i] = 'X'
		}
		err := dir.ReadNext(&fi)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		name := fi.Name()
		if strings.IndexByte(name, 0) >= 0 {
			t.Errorf("Name %q contains NUL", name)
		}
		if got := string(fi.AppendName(nil)); got != name {
			t.Errorf("AppendName = %q, Name = %q", got, name)
		}
		matched := false
		for want, dup := range seen {
			// Case-insensitive: the fat_nolfn build reports the uppercase SFN.
			if strings.EqualFold(name, want) {
				if dup {
					t.Errorf("entry %q listed twice", name)
				}
				seen[want] = true
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("unexpected name %q (stale bytes past terminator?)", name)
		}
	}
	for want, ok := range seen {
		if !ok {
			t.Errorf("entry %q not listed", want)
		}
	}
}
