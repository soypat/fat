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
// the exported high-level API (Mount, OpenFile, Write, Read, Seek, Close,
// Sync, Remove) starting from a freshly formatted image produced by the
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
	if _, err := f.Write([]byte(content)); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
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
}

// spotCheckSmall re-mounts the device read-only and verifies file contents
// through the read API before the byte-level image comparison.
func spotCheckSmall(t *testing.T, dev *BlockByteSlice) {
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatalf("verify re-mount: %v", err)
	}
	// c.dat: 6 clusters tag 4, overwritten at 12345 with 5000 tag-6 bytes,
	// stretched to 43000 with tag-7 tail at 40000.
	got := readAllFile(t, &fsys, "c.dat")
	if len(got) != 43000 {
		t.Fatalf("c.dat size = %d, want 43000", len(got))
	}
	for i, b := range got[:12345] {
		if b != pat(4, i) {
			t.Fatalf("c.dat[%d] = %#02x, want tag-4 pattern %#02x", i, b, pat(4, i))
		}
	}
	for i := 0; i < 5000; i++ {
		if got[12345+i] != pat(6, i) {
			t.Fatalf("c.dat[%d] tag-6 mismatch", 12345+i)
		}
	}
	for i := 0; i < 3000; i++ {
		if got[40000+i] != pat(7, i) {
			t.Fatalf("c.dat[%d] tag-7 mismatch", 40000+i)
		}
	}
	if got := readAllFile(t, &fsys, "ñandú.txt"); len(got) != 100 || got[99] != pat(8, 99) {
		t.Fatalf("ñandú.txt content mismatch (len %d)", len(got))
	}
	if got := readAllFile(t, &fsys, "😀emoji😀.txt"); len(got) != 50 {
		t.Fatalf("emoji file length = %d, want 50", len(got))
	}
	if got := readAllFile(t, &fsys, "a.dat"); len(got) != 2*4096 || got[0] != pat(5, 0) {
		t.Fatalf("a.dat not truncated/rewritten correctly (len %d)", len(got))
	}
	var f File
	if err := fsys.OpenFile(&f, "b.dat", ModeRead); err == nil {
		t.Fatal("b.dat still exists after Remove")
	}
	if err := fsys.OpenFile(&f, "collision test file 07.dat", ModeRead); err == nil {
		t.Fatal("collision test file 07.dat still exists after Remove")
	}
	if got := readAllFile(t, &fsys, "collision test file 08.dat"); string(got) != "collide" {
		t.Fatalf("collision test file 08.dat = %q", got)
	}
}

func TestGoldenTortureFAT12(t *testing.T) {
	dev := goldenDevice(t, "golden-fmt12.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if fsys.fstype != fstypeFAT12 {
		t.Fatalf("fstype = %d, want FAT12", fsys.fstype)
	}
	tortureScriptSmall(t, &fsys)
	if err := fsys.Sync(); err != nil {
		t.Fatalf("fs.Sync: %v", err)
	}
	spotCheckSmall(t, dev)
	compareGolden(t, dev, "golden-torture12.img")
}

func TestGoldenTortureFAT16(t *testing.T) {
	dev := goldenDevice(t, "golden-fmt16.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if fsys.fstype != fstypeFAT16 {
		t.Fatalf("fstype = %d, want FAT16", fsys.fstype)
	}
	tortureScriptSmall(t, &fsys)
	if err := fsys.Sync(); err != nil {
		t.Fatalf("fs.Sync: %v", err)
	}
	spotCheckSmall(t, dev)
	compareGolden(t, dev, "golden-torture16.img")
}

// TestGoldenTortureFAT32 mirrors script32() in mkgolden.c: the 512-byte
// clusters make the FAT32 root directory a cluster chain that stretches
// repeatedly (dir_clear/disk_erase), and long chains cross FAT sector
// boundaries during allocation and removal.
func TestGoldenTortureFAT32(t *testing.T) {
	dev := goldenDevice(t, "golden-fmt32.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if fsys.fstype != fstypeFAT32 {
		t.Fatalf("fstype = %d, want FAT32", fsys.fstype)
	}

	// S1: 140 LFN files in the root directory force repeated root-dir
	// stretch (each file needs 4 dir entries = 1/4 cluster).
	for i := 0; i < 140; i++ {
		writeStr(t, &fsys, fmt.Sprintf("root%03d file with a long name.txt", i), fmt.Sprintf("file %03d", i))
	}

	// S2: big.dat = 100000 bytes (tag 1), ~196 cluster chain.
	createPat(t, &fsys, "big.dat", 1, 100000)

	// S3: remove every third root file: punches LFN-block holes in the
	// stretched root directory and frees clusters.
	for i := 0; i < 140; i += 3 {
		name := fmt.Sprintf("root%03d file with a long name.txt", i)
		if err := fsys.Remove(name); err != nil {
			t.Fatalf("remove %s: %v", name, err)
		}
	}

	// S4: append 50000 bytes to big.dat.
	appendPat(t, &fsys, "big.dat", 1, 100000, 50000)

	// S5: huge.dat = 150000 bytes (tag 2).
	createPat(t, &fsys, "huge.dat", 2, 150000)

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
	createPat(t, &fsys, "big2.dat", 5, 200000)

	if err := fsys.Sync(); err != nil {
		t.Fatalf("fs.Sync: %v", err)
	}

	// Spot-check via read API before the byte-level comparison.
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatalf("verify re-mount: %v", err)
	}
	got := readAllFile(t, &fsys, "big.dat")
	if len(got) != 150000 {
		t.Fatalf("big.dat size = %d, want 150000", len(got))
	}
	for i := 0; i < 8000; i++ {
		if got[99991+i] != pat(3, i) {
			t.Fatalf("big.dat[%d] tag-3 mismatch", 99991+i)
		}
	}
	if got := readAllFile(t, &fsys, "big2.dat"); len(got) != 200000 || got[199999] != pat(5, 199999) {
		t.Fatalf("big2.dat content mismatch (len %d)", len(got))
	}
	if err := fsys.OpenFile(&f, "huge.dat", ModeRead); err == nil {
		t.Fatal("huge.dat still exists after Remove")
	}

	compareGolden(t, dev, "golden-torture32.img")
}
