package fat

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// The golden torture tests replay deterministic operation scripts using only
// the exported high-level API (Mount, OpenFile, Write, WriteString, WriteAt,
// Read, ReadAt, Seek, Size, Truncate, Close, Sync, Remove, Mkdir, Rename,
// Stat, Unmount) starting from a freshly formatted image produced by the
// original FatFs ff16 library. The resulting image is compared byte-for-byte
// with the corresponding golden image produced by the companion C program
// testdata/mkgolden.c which drives ff16 through the identical script.
// The numbered steps must stay in lockstep with mkgolden.c; see its header
// for how to regenerate the images.

// goldenImage returns the decompressed contents of a golden image.
func goldenImage(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err == nil {
		return raw
	}
	f, err := os.Open(filepath.Join("testdata", name+".gz"))
	if err != nil {
		t.Fatalf("read golden image %s(.gz): %v", name, err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gunzip %s.gz: %v", name, err)
	}
	data, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip %s.gz: %v", name, err)
	}
	return data
}

// goldenDevice wraps a golden baseline image in a BlockDevice.
func goldenDevice(t *testing.T, baseline string) *BlockByteSlice {
	t.Helper()
	blk, err := makeBlockIndexer(512)
	if err != nil {
		t.Fatal(err)
	}
	return &BlockByteSlice{blk: blk, buf: goldenImage(t, baseline)}
}

// compareGolden requires the device contents to match the golden torture
// image byte for byte, reporting the first mismatch with sector context.
func compareGolden(t *testing.T, dev *BlockByteSlice, torture string) {
	t.Helper()
	want := goldenImage(t, torture)
	if len(want) != len(dev.buf) {
		t.Fatalf("image size %d, device size %d", len(want), len(dev.buf))
	}
	diffs := 0
	for i := range want {
		if dev.buf[i] != want[i] {
			if diffs == 0 {
				t.Errorf("first mismatch at offset %#x (sector %d, off %d): got %#02x, want %#02x",
					i, i/512, i%512, dev.buf[i], want[i])
			}
			diffs++
		}
	}
	if diffs > 0 {
		t.Fatalf("%d/%d bytes differ from C golden image %s", diffs, len(want), torture)
	}
}

// pat returns the deterministic byte at offset i for the given tag.
// Must match the C implementation in mkgolden.c.
func pat(tag, i int) byte {
	return byte(i*31 + tag*17 + 7)
}

// writePat writes n patterned bytes to f; the pattern index is the absolute
// file offset start+.. so content is chunk-size independent.
func writePat(t *testing.T, f *File, tag, start, n int) {
	t.Helper()
	buf := make([]byte, 512)
	for done := 0; done < n; {
		c := n - done
		if c > len(buf) {
			c = len(buf)
		}
		for j := 0; j < c; j++ {
			buf[j] = pat(tag, start+done+j)
		}
		if _, err := f.Write(buf[:c]); err != nil {
			t.Fatalf("writePat(tag=%d): %v", tag, err)
		}
		done += c
	}
}

// createPat creates/truncates a file and writes n patterned bytes.
func createPat(t *testing.T, fsys *FS, name string, tag, n int) {
	t.Helper()
	var f File
	if err := fsys.OpenFile(&f, name, ModeCreateAlways|ModeWrite); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	writePat(t, &f, tag, 0, n)
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

// appendPat opens an existing file in append mode and writes n bytes
// continuing the pattern at offset start (must equal current file size).
func appendPat(t *testing.T, fsys *FS, name string, tag, start, n int) {
	t.Helper()
	var f File
	if err := fsys.OpenFile(&f, name, ModeOpenAppend|ModeWrite); err != nil {
		t.Fatalf("append-open %s: %v", name, err)
	}
	if f.obj.objsize != int64(start) {
		t.Fatalf("appendPat %s: size %d != start %d", name, f.obj.objsize, start)
	}
	writePat(t, &f, tag, start, n)
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

func writeStr(t *testing.T, fsys *FS, name, content string) {
	t.Helper()
	var f File
	if err := fsys.OpenFile(&f, name, ModeCreateAlways|ModeWrite); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

// writeAtPat writes n patterned bytes (pattern index starting at 0) at
// absolute file offset off using WriteAt.
func writeAtPat(t *testing.T, f *File, tag int, off int64, n int) {
	t.Helper()
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = pat(tag, i)
	}
	if nw, err := f.WriteAt(buf, off); err != nil || nw != n {
		t.Fatalf("WriteAt(tag=%d, off=%d): n=%d err=%v", tag, off, nw, err)
	}
}

// checkPatAt reads n bytes at offset off via ReadAt and requires them to
// match the tag pattern starting at pattern index patStart.
func checkPatAt(t *testing.T, f *File, name string, tag int, off int64, n, patStart int) {
	t.Helper()
	buf := make([]byte, n)
	if nr, err := f.ReadAt(buf, off); err != nil || nr != n {
		t.Fatalf("ReadAt %s off=%d: n=%d err=%v", name, off, nr, err)
	}
	for i, b := range buf {
		if b != pat(tag, patStart+i) {
			t.Fatalf("%s[%d] = %#02x, want tag-%d pattern %#02x", name, off+int64(i), b, tag, pat(tag, patStart+i))
		}
	}
}

func readAllFile(t *testing.T, fsys *FS, name string) []byte {
	t.Helper()
	var f File
	if err := fsys.OpenFile(&f, name, ModeRead); err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, &f); err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
	return buf.Bytes()
}

// tortureScriptSmall mirrors script_small() in mkgolden.c: reallocation,
// fragmentation, CreateAlways truncation, seek-overwrite, chain stretch via
// seek past EOF, SFN collision bursts, unicode LFNs and directory slot reuse.
// Sizes are in bytes; comments assume the FAT12 image's 4096-byte clusters.
func tortureScriptSmall(t *testing.T, fsys *FS) {
	// S1: a.dat = 3 clusters (tag 1).
	createPat(t, fsys, "a.dat", 1, 3*4096)

	// S2: b.dat = 2 clusters (tag 2).
	createPat(t, fsys, "b.dat", 2, 2*4096)

	// S3: extend a.dat by 2 clusters in append mode.
	appendPat(t, fsys, "a.dat", 1, 3*4096, 2*4096)

	// S4: frag.dat = 1 cluster, then append 1 more (fragmentation).
	createPat(t, fsys, "frag.dat", 3, 4096)
	appendPat(t, fsys, "frag.dat", 3, 4096, 4096)

	// S5: interleaved sub-cluster appends to a.dat/b.dat (4 rounds of
	// 1024B each) so their cluster chains interleave as they grow.
	for i := 0; i < 4; i++ {
		appendPat(t, fsys, "a.dat", 1, 5*4096+i*1024, 1024)
		appendPat(t, fsys, "b.dat", 2, 2*4096+i*1024, 1024)
	}

	// S6: punch FAT holes.
	if err := fsys.Remove("b.dat"); err != nil {
		t.Fatalf("remove b.dat: %v", err)
	}
	if err := fsys.Remove("frag.dat"); err != nil {
		t.Fatalf("remove frag.dat: %v", err)
	}

	// S7: c.dat = 6 clusters threads through the holes (tag 4).
	createPat(t, fsys, "c.dat", 4, 6*4096)

	// S8: re-create a.dat with CreateAlways (truncates old chain, allocator
	// reuses the hole) and write 2 clusters (tag 5).
	createPat(t, fsys, "a.dat", 5, 2*4096)

	// S9: mid-file overwrite of c.dat at misaligned offset 12345 (tag 6),
	// then seek past EOF to 40000 and write 3000 (tag 7): chain stretch.
	var f File
	if err := fsys.OpenFile(&f, "c.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open c.dat: %v", err)
	}
	if _, err := f.Seek(12345, io.SeekStart); err != nil {
		t.Fatalf("seek c.dat 12345: %v", err)
	}
	writePat(t, &f, 6, 0, 5000)
	if _, err := f.Seek(40000, io.SeekStart); err != nil {
		t.Fatalf("seek c.dat 40000: %v", err)
	}
	writePat(t, &f, 7, 0, 3000)
	if err := f.Sync(); err != nil {
		t.Fatalf("sync c.dat: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close c.dat: %v", err)
	}

	// S10: 12 LFNs colliding on the same 8.3 stem: numbered ~1..~5 then
	// hashed short names.
	for i := 1; i <= 12; i++ {
		writeStr(t, fsys, fmt.Sprintf("collision test file %02d.dat", i), "collide")
	}

	// S11: unicode LFNs (CP437 SFN conversion incl. lossy and surrogates).
	for _, uf := range []struct {
		name string
		n    int
	}{
		{"ñandú.txt", 100},
		{"曲がり角.txt", 200},
		{"αβγδε.dat", 300},
		{"😀emoji😀.txt", 50},
	} {
		var f File
		if err := fsys.OpenFile(&f, uf.name, ModeCreateAlways|ModeWrite); err != nil {
			t.Fatalf("create %s: %v", uf.name, err)
		}
		writePat(t, &f, 8, 0, uf.n)
		if err := f.Close(); err != nil {
			t.Fatalf("close %s: %v", uf.name, err)
		}
	}

	// S12: 32 small files.
	for i := 0; i < 32; i++ {
		writeStr(t, fsys, fmt.Sprintf("small%02d.txt", i), "hello")
	}

	// S13: delete every other small file.
	for i := 0; i < 32; i += 2 {
		name := fmt.Sprintf("small%02d.txt", i)
		if err := fsys.Remove(name); err != nil {
			t.Fatalf("remove %s: %v", name, err)
		}
	}

	// S14: 8 new files reuse the freed directory slots.
	for i := 0; i < 8; i++ {
		writeStr(t, fsys, fmt.Sprintf("renew%02d.txt", i), "world!")
	}

	// S15: delete an LFN entry block in the middle of the directory.
	if err := fsys.Remove("collision test file 07.dat"); err != nil {
		t.Fatalf("remove collision 07: %v", err)
	}

	// S16: sub-directories: subdir and nested one inside it.
	if err := fsys.Mkdir("subdir"); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	if err := fsys.Mkdir("subdir/nested"); err != nil {
		t.Fatalf("mkdir subdir/nested: %v", err)
	}

	// S17: files inside the new directories.
	writeStr(t, fsys, "subdir/deep.txt", "deep blue")
	writeStr(t, fsys, "subdir/nested/leaf.txt", "leaf")

	// S18: positional writes on c.dat: 1500 bytes at 2000 (tag 9) inside
	// the file and 1000 bytes at 44000 (tag 10) past EOF 43000, extending
	// the file to 45000.
	if err := fsys.OpenFile(&f, "c.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open c.dat writeat: %v", err)
	}
	writeAtPat(t, &f, 9, 2000, 1500)
	writeAtPat(t, &f, 10, 44000, 1000)
	if f.Size() != 45000 {
		t.Fatalf("c.dat Size() = %d, want 45000", f.Size())
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close c.dat writeat: %v", err)
	}

	// S19: truncation: c.dat 45000 -> 20000 (drops chain tail), a.dat -> 0
	// (removes whole chain) then rewrite as "reborn".
	if err := fsys.OpenFile(&f, "c.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open c.dat trunc: %v", err)
	}
	if err := f.Truncate(20000); err != nil {
		t.Fatalf("truncate c.dat: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close c.dat trunc: %v", err)
	}
	if err := fsys.OpenFile(&f, "a.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open a.dat trunc: %v", err)
	}
	if err := f.Truncate(0); err != nil {
		t.Fatalf("truncate a.dat: %v", err)
	}
	if _, err := f.WriteString("reborn"); err != nil {
		t.Fatalf("rewrite a.dat: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close a.dat trunc: %v", err)
	}

	// S20: rename within the root directory.
	if err := fsys.Rename("c.dat", "cc.dat"); err != nil {
		t.Fatalf("rename c.dat: %v", err)
	}

	// S21: move an LFN file into a sub-directory.
	if err := fsys.Rename("collision test file 08.dat", "subdir/collision east.dat"); err != nil {
		t.Fatalf("move collision 08: %v", err)
	}

	// S22: move a directory to the root: its .. entry must be rewritten.
	// Then create a file through the moved path.
	if err := fsys.Rename("subdir/nested", "nested2"); err != nil {
		t.Fatalf("move nested: %v", err)
	}
	writeStr(t, fsys, "nested2/after.txt", "moved dir works")
}

// spotCheckSmall re-mounts the device read-only and verifies file contents
// through the read API before the byte-level image comparison.
func spotCheckSmall(t *testing.T, dev *BlockByteSlice) {
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatalf("verify re-mount: %v", err)
	}
	// cc.dat (renamed from c.dat): tag 4 base, tag-9 overwrite at 2000,
	// tag-6 overwrite at 12345, truncated from 45000 to 20000.
	var f File
	if err := fsys.OpenFile(&f, "cc.dat", ModeRead); err != nil {
		t.Fatalf("open cc.dat: %v", err)
	}
	if f.Size() != 20000 {
		t.Fatalf("cc.dat Size() = %d, want 20000", f.Size())
	}
	checkPatAt(t, &f, "cc.dat", 4, 0, 2000, 0)
	checkPatAt(t, &f, "cc.dat", 9, 2000, 1500, 0)
	checkPatAt(t, &f, "cc.dat", 4, 3500, 12345-3500, 3500)
	checkPatAt(t, &f, "cc.dat", 6, 12345, 5000, 0)
	checkPatAt(t, &f, "cc.dat", 4, 17345, 20000-17345, 17345)
	// ReadAt straddling EOF returns the short count and io.EOF.
	buf := make([]byte, 20)
	if n, err := f.ReadAt(buf, 19990); n != 10 || err != io.EOF {
		t.Fatalf("cc.dat ReadAt at EOF: n=%d err=%v, want 10, io.EOF", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close cc.dat: %v", err)
	}
	if err := fsys.OpenFile(&f, "c.dat", ModeRead); err == nil {
		t.Fatal("c.dat still exists after Rename")
	}
	if got := readAllFile(t, &fsys, "ñandú.txt"); len(got) != 100 || got[99] != pat(8, 99) {
		t.Fatalf("ñandú.txt content mismatch (len %d)", len(got))
	}
	if got := readAllFile(t, &fsys, "😀emoji😀.txt"); len(got) != 50 {
		t.Fatalf("emoji file length = %d, want 50", len(got))
	}
	// a.dat: truncated to zero then rewritten via WriteString.
	if got := readAllFile(t, &fsys, "a.dat"); string(got) != "reborn" {
		t.Fatalf("a.dat = %q, want \"reborn\"", got)
	}
	if err := fsys.OpenFile(&f, "b.dat", ModeRead); err == nil {
		t.Fatal("b.dat still exists after Remove")
	}
	if err := fsys.OpenFile(&f, "collision test file 07.dat", ModeRead); err == nil {
		t.Fatal("collision test file 07.dat still exists after Remove")
	}
	// collision 08 moved into subdir.
	if err := fsys.OpenFile(&f, "collision test file 08.dat", ModeRead); err == nil {
		t.Fatal("collision test file 08.dat still in root after Rename")
	}
	if got := readAllFile(t, &fsys, "subdir/collision east.dat"); string(got) != "collide" {
		t.Fatalf("subdir/collision east.dat = %q", got)
	}
	// Directory tree checks through Stat.
	var info FileInfo
	if err := fsys.Stat("subdir", &info); err != nil || !info.IsDir() {
		t.Fatalf("Stat subdir: IsDir=%v err=%v", info.IsDir(), err)
	}
	if err := fsys.Stat("subdir/deep.txt", &info); err != nil || info.IsDir() || info.Size() != 9 {
		t.Fatalf("Stat subdir/deep.txt: IsDir=%v Size=%d err=%v", info.IsDir(), info.Size(), err)
	}
	if err := fsys.Stat("subdir/nested", &info); err == nil {
		t.Fatal("subdir/nested still exists after directory Rename")
	}
	if got := readAllFile(t, &fsys, "nested2/leaf.txt"); string(got) != "leaf" {
		t.Fatalf("nested2/leaf.txt = %q", got)
	}
	if got := readAllFile(t, &fsys, "nested2/after.txt"); string(got) != "moved dir works" {
		t.Fatalf("nested2/after.txt = %q", got)
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("verify Unmount: %v", err)
	}
}

// skipIfNoLFN skips golden torture tests on the fat_nolfn build: the scripts
// are full of long and unicode file names that only exist with LFN support.
func skipIfNoLFN(t *testing.T) {
	t.Helper()
	if !lfnEnabled {
		t.Skip("golden torture script requires LFN support (built with fat_nolfn)")
	}
}

// skipIfNoExFAT skips exFAT tests on fat_noexfat/fat_nolfn builds.
func skipIfNoExFAT(t *testing.T) {
	t.Helper()
	if !exfatEnabled {
		t.Skip("test requires exFAT support (built with fat_noexfat or fat_nolfn)")
	}
}

func TestGoldenTortureFAT12(t *testing.T) {
	skipIfNoLFN(t)
	dev := goldenDevice(t, "golden-fmt12.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if fsys.fstype != fstypeFAT12 {
		t.Fatalf("fstype = %d, want FAT12", fsys.fstype)
	}
	tortureScriptSmall(t, &fsys)
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("fs.Unmount: %v", err)
	}
	spotCheckSmall(t, dev)
	compareGolden(t, dev, "golden-torture12.img")
}

func TestGoldenTortureFAT16(t *testing.T) {
	skipIfNoLFN(t)
	dev := goldenDevice(t, "golden-fmt16.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if fsys.fstype != fstypeFAT16 {
		t.Fatalf("fstype = %d, want FAT16", fsys.fstype)
	}
	tortureScriptSmall(t, &fsys)
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("fs.Unmount: %v", err)
	}
	spotCheckSmall(t, dev)
	compareGolden(t, dev, "golden-torture16.img")
}

// TestGoldenTortureFAT32 mirrors script32() in mkgolden.c: the 512-byte
// clusters make the FAT32 root directory a cluster chain that stretches
// repeatedly (dir_clear/disk_erase), and long chains cross FAT sector
// boundaries during allocation and removal.
func TestGoldenTortureFAT32(t *testing.T) {
	skipIfNoLFN(t)
	dev := goldenDevice(t, "golden-fmt32.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if fsys.fstype != fstypeFAT32 {
		t.Fatalf("fstype = %d, want FAT32", fsys.fstype)
	}
	tortureScript32(t, &fsys)
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("fs.Unmount: %v", err)
	}
	spotCheck32(t, dev)
	compareGolden(t, dev, "golden-torture32.img")
}

// tortureScript32 mirrors script32() in mkgolden.c and is shared by the
// FAT32 and exFAT golden torture tests (both volumes use 512B clusters).
func tortureScript32(t *testing.T, fsys *FS) {
	// S1: 140 LFN files in the root directory force repeated root-dir
	// stretch (each file needs 4 dir entries = 1/4 cluster).
	for i := 0; i < 140; i++ {
		writeStr(t, fsys, fmt.Sprintf("root%03d file with a long name.txt", i), fmt.Sprintf("file %03d", i))
	}

	// S2: big.dat = 100000 bytes (tag 1), ~196 cluster chain.
	createPat(t, fsys, "big.dat", 1, 100000)

	// S3: remove every third root file: punches LFN-block holes in the
	// stretched root directory and frees clusters.
	for i := 0; i < 140; i += 3 {
		name := fmt.Sprintf("root%03d file with a long name.txt", i)
		if err := fsys.Remove(name); err != nil {
			t.Fatalf("remove %s: %v", name, err)
		}
	}

	// S4: append 50000 bytes to big.dat.
	appendPat(t, fsys, "big.dat", 1, 100000, 50000)

	// S5: huge.dat = 150000 bytes (tag 2).
	createPat(t, fsys, "huge.dat", 2, 150000)

	// S6: mid-file overwrite of big.dat at misaligned offset 99991 (tag 3).
	var f File
	if err := fsys.OpenFile(&f, "big.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open big.dat: %v", err)
	}
	if _, err := f.Seek(99991, io.SeekStart); err != nil {
		t.Fatalf("seek big.dat: %v", err)
	}
	writePat(t, &f, 3, 0, 8000)
	if err := f.Close(); err != nil {
		t.Fatalf("close big.dat: %v", err)
	}

	// S7: 30 new LFN files fill root-dir holes and stretch further.
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("new%03d with another long name.bin", i)
		var f File
		if err := fsys.OpenFile(&f, name, ModeCreateAlways|ModeWrite); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		writePat(t, &f, 4, i*64, 64)
		if err := f.Close(); err != nil {
			t.Fatalf("close %s: %v", name, err)
		}
	}

	// S8: free a long chain.
	if err := fsys.Remove("huge.dat"); err != nil {
		t.Fatalf("remove huge.dat: %v", err)
	}

	// S9: big2.dat = 200000 bytes (tag 5) wraps into the freed chain.
	createPat(t, fsys, "big2.dat", 5, 200000)

	// S10: logs directory with 24 LFN files: the sub-directory table is a
	// 512B-cluster chain that stretches repeatedly (24 files x 4 entries
	// = 96 entries = 6 clusters).
	if err := fsys.Mkdir("logs"); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	for i := 0; i < 24; i++ {
		writeStr(t, fsys, fmt.Sprintf("logs/log entry number %03d.txt", i), fmt.Sprintf("entry %03d", i))
	}

	// S11: positional writes on big2.dat: 4000 bytes at 123456 (tag 6)
	// inside the file and 2000 bytes at EOF 200000 (tag 7), extending it
	// to 202000.
	if err := fsys.OpenFile(&f, "big2.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open big2.dat writeat: %v", err)
	}
	writeAtPat(t, &f, 6, 123456, 4000)
	writeAtPat(t, &f, 7, 200000, 2000)
	if f.Size() != 202000 {
		t.Fatalf("big2.dat Size() = %d, want 202000", f.Size())
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close big2.dat writeat: %v", err)
	}

	// S12: truncate big2.dat 202000 -> 150001 (misaligned; frees a long
	// chain tail).
	if err := fsys.OpenFile(&f, "big2.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open big2.dat trunc: %v", err)
	}
	if err := f.Truncate(150001); err != nil {
		t.Fatalf("truncate big2.dat: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close big2.dat trunc: %v", err)
	}

	// S13: move big.dat into the stretched logs directory.
	if err := fsys.Rename("big.dat", "logs/big.dat"); err != nil {
		t.Fatalf("move big.dat: %v", err)
	}

	// S14: move a sub-directory of logs to the root: its .. entry changes
	// from the logs cluster to 0 (root). Then create a file through the
	// moved path.
	if err := fsys.Mkdir("logs/inner"); err != nil {
		t.Fatalf("mkdir logs/inner: %v", err)
	}
	if err := fsys.Rename("logs/inner", "inner"); err != nil {
		t.Fatalf("move inner: %v", err)
	}
	writeStr(t, fsys, "inner/done.txt", "ok")
}

// spotCheck32 re-mounts the device read-only and verifies file contents
// through the read API before the byte-level image comparison. Shared by
// the FAT32 and exFAT golden torture tests.
func spotCheck32(t *testing.T, dev *BlockByteSlice) {
	var fsys FS
	var f File
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatalf("verify re-mount: %v", err)
	}
	if err := fsys.OpenFile(&f, "big.dat", ModeRead); err == nil {
		t.Fatal("big.dat still in root after Rename")
	}
	got := readAllFile(t, &fsys, "logs/big.dat")
	if len(got) != 150000 {
		t.Fatalf("logs/big.dat size = %d, want 150000", len(got))
	}
	for i := 0; i < 8000; i++ {
		if got[99991+i] != pat(3, i) {
			t.Fatalf("logs/big.dat[%d] tag-3 mismatch", 99991+i)
		}
	}
	// big2.dat: tag 5 base, tag-6 overwrite at 123456, extension truncated
	// away at 150001.
	if err := fsys.OpenFile(&f, "big2.dat", ModeRead); err != nil {
		t.Fatalf("open big2.dat: %v", err)
	}
	if f.Size() != 150001 {
		t.Fatalf("big2.dat Size() = %d, want 150001", f.Size())
	}
	checkPatAt(t, &f, "big2.dat", 6, 123456, 4000, 0)
	checkPatAt(t, &f, "big2.dat", 5, 150000, 1, 150000)
	if err := f.Close(); err != nil {
		t.Fatalf("close big2.dat: %v", err)
	}
	if err := fsys.OpenFile(&f, "huge.dat", ModeRead); err == nil {
		t.Fatal("huge.dat still exists after Remove")
	}
	var info FileInfo
	if err := fsys.Stat("inner", &info); err != nil || !info.IsDir() {
		t.Fatalf("Stat inner: IsDir=%v err=%v", info.IsDir(), err)
	}
	if err := fsys.Stat("logs/inner", &info); err == nil {
		t.Fatal("logs/inner still exists after directory Rename")
	}
	if got := readAllFile(t, &fsys, "inner/done.txt"); string(got) != "ok" {
		t.Fatalf("inner/done.txt = %q", got)
	}
	if got := readAllFile(t, &fsys, "logs/log entry number 013.txt"); string(got) != "entry 013" {
		t.Fatalf("log entry 013 = %q", got)
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("verify Unmount: %v", err)
	}
}
