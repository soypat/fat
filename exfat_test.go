package fat

import (
	"testing"
)

// initTestExFAT formats a fresh in-memory exFAT volume of numBlocks 512-byte
// blocks (min 0x1000) with the Go formatter and mounts it read-write, with
// /rootdir created to mirror the FAT test image layout. Callers must have
// checked exfatEnabled.
func initTestExFAT(numBlocks int) (*FS, *BlockByteSlice, error) {
	blk, err := makeBlockIndexer(512)
	if err != nil {
		return nil, nil, err
	}
	dev := &BlockByteSlice{blk: blk, buf: make([]byte, numBlocks*512)}
	var fmtr Formatter
	err = fmtr.Format(dev, 512, numBlocks, FormatParams{Format: FormatExFAT})
	if err != nil {
		return nil, nil, err
	}
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		return nil, nil, err
	}
	if err := fsys.Mkdir("rootdir"); err != nil {
		return nil, nil, err
	}
	return &fsys, dev, nil
}

// TestGoldenTortureExFAT mirrors the exFAT pair in mkgolden.c: the FAT32
// torture script (shared, 512B clusters) plus script_exfat() which exercises
// exFAT-specific machinery — the NoFatChain contiguous state and its
// transition to a materialized FAT chain on fragmentation, bitmap hole
// reuse, name-hash recompute on rename and dot-entry-free directories.
// The result must match the C-generated image byte for byte.
func TestGoldenTortureExFAT(t *testing.T) {
	skipIfNoExFAT(t)
	dev := goldenDevice(t, "golden-fmtex.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if fsys.fstype != FormatExFAT {
		t.Fatalf("fstype = %d, want exFAT", fsys.fstype)
	}
	tortureScript32(t, &fsys)
	tortureScriptExFAT(t, &fsys)
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("fs.Unmount: %v", err)
	}
	spotCheck32(t, dev)
	spotCheckExFAT(t, dev)
	compareGolden(t, dev, "golden-tortureex.img")
}

// tortureScriptExFAT mirrors script_exfat() in mkgolden.c; steps must stay
// in lockstep. Runs after tortureScript32 on the exFAT volume.
func tortureScriptExFAT(t *testing.T, fsys *FS) {
	// X1: cont.dat = 20000 bytes (tag 11): contiguous, NoFatChain.
	// block.dat = 10000 bytes (tag 12) allocated right after it.
	createPat(t, fsys, "cont.dat", 11, 20000)
	createPat(t, fsys, "block.dat", 12, 10000)

	// X2: append 20000 bytes to cont.dat: the next free cluster is beyond
	// block.dat so the file fragments and its FAT chain must be
	// materialized (stat 2 -> 3 -> chain on FAT).
	appendPat(t, fsys, "cont.dat", 11, 20000, 20000)

	// X3: truncate cont.dat 40000 -> 5000: drops the fragmented tail; the
	// remaining head is contiguous again.
	var f File
	if err := fsys.OpenFile(&f, "cont.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open cont.dat trunc: %v", err)
	}
	if err := f.Truncate(5000); err != nil {
		t.Fatalf("truncate cont.dat: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close cont.dat trunc: %v", err)
	}

	// X4: mid-file overwrite of block.dat (misaligned, stays contiguous:
	// NoFatChain must survive an in-place rewrite).
	if err := fsys.OpenFile(&f, "block.dat", ModeRW|ModeOpenExisting); err != nil {
		t.Fatalf("open block.dat: %v", err)
	}
	if _, err := f.Seek(1234, 0); err != nil {
		t.Fatalf("seek block.dat: %v", err)
	}
	writePat(t, &f, 13, 0, 4000)
	if err := f.Close(); err != nil {
		t.Fatalf("close block.dat: %v", err)
	}

	// X5: rename with a unicode name: exFAT name hash recompute.
	if err := fsys.Rename("cont.dat", "contiñuación.dat"); err != nil {
		t.Fatalf("rename cont.dat: %v", err)
	}

	// X6: deep directory nesting (exFAT directories have no dot entries).
	for _, d := range []string{"deep", "deep/deeper", "deep/deeper/deepest"} {
		if err := fsys.Mkdir(d); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	writeStr(t, fsys, "deep/deeper/deepest/bottom.txt", "made it")

	// X7: delete-and-reuse: free block.dat's clusters then thread
	// refill.dat = 15000 bytes (tag 14) through the bitmap hole.
	if err := fsys.Remove("block.dat"); err != nil {
		t.Fatalf("remove block.dat: %v", err)
	}
	createPat(t, fsys, "refill.dat", 14, 15000)

	// X8: move the renamed unicode file into the deep directory.
	if err := fsys.Rename("contiñuación.dat", "deep/contiñuación.dat"); err != nil {
		t.Fatalf("move unicode: %v", err)
	}
}

// TestFormatExFATGolden formats a blank 64MiB device with the same
// parameters as mkgolden.c's f_mkfs(FM_EXFAT|FM_SFD, au 512) and requires
// the result to match golden-fmtex.img byte for byte, then mounts it and
// runs the full torture scripts as an end-to-end check.
func TestFormatExFATGolden(t *testing.T) {
	skipIfNoExFAT(t)
	want := goldenImage(t, "golden-fmtex.img")
	blk, err := makeBlockIndexer(512)
	if err != nil {
		t.Fatal(err)
	}
	dev := &BlockByteSlice{blk: blk, buf: make([]byte, len(want))}
	var fmtr Formatter
	err = fmtr.Format(dev, 512, len(want)/512, FormatParams{
		Format:      FormatExFAT,
		ClusterSize: 1, // 1 block = 512 byte clusters, matching mkgolden.c au 512.
	})
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	compareGolden(t, dev, "golden-fmtex.img")

	// The formatted volume must behave identically to the C-formatted one.
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount formatted volume: %v", err)
	}
	tortureScript32(t, &fsys)
	tortureScriptExFAT(t, &fsys)
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	compareGolden(t, dev, "golden-tortureex.img")
}

// TestExFATBigFile exercises the 64-bit file size support: on a sparse
// 5GiB device a file is stretched past the 4GiB FAT limit via WriteAt,
// synced (64-bit sizes in the entry set) and read back. The whole chain is
// contiguous (NoFatChain) so FAT values are generated, not stored.
func TestExFATBigFile(t *testing.T) {
	skipIfNoExFAT(t)
	if testing.Short() {
		t.Skip("big file test, skipped in -short")
	}
	const gib = int64(1) << 30
	const devSize = 5 * gib
	dev := &BlockMap{size: devSize}
	var fmtr Formatter
	err := fmtr.Format(dev, 512, int(devSize/512), FormatParams{Format: FormatExFAT})
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	const off = 4*gib + 12345 // Past the 4GiB-1 FATxx file size limit.
	const n = 4000
	var f File
	if err := fsys.OpenFile(&f, "big.bin", ModeCreateAlways|ModeRW); err != nil {
		t.Fatalf("create big.bin: %v", err)
	}
	writeAtPat(t, &f, 1, off, n)
	if f.Size() != off+n {
		t.Fatalf("Size() = %d, want %d", f.Size(), off+n)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close big.bin: %v", err)
	}

	var info FileInfo
	if err := fsys.Stat("big.bin", &info); err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != off+n {
		t.Fatalf("Stat size = %d, want %d (64-bit size lost)", info.Size(), off+n)
	}
	if err := fsys.OpenFile(&f, "big.bin", ModeRead); err != nil {
		t.Fatalf("reopen big.bin: %v", err)
	}
	checkPatAt(t, &f, "big.bin", 1, off, n, 0)
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	t.Logf("sparse device holds %d blocks (%d KiB) for a %.2f GiB file",
		len(dev.data), len(dev.data)/2, float64(off+n)/float64(gib))
}

// spotCheckExFAT verifies the exFAT-specific script results through the
// read API before the byte-level comparison.
func spotCheckExFAT(t *testing.T, dev *BlockByteSlice) {
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatalf("verify re-mount: %v", err)
	}
	uni := readAllFile(t, &fsys, "deep/contiñuación.dat")
	if len(uni) != 5000 {
		t.Fatalf("contiñuación.dat len = %d, want 5000", len(uni))
	}
	for i, b := range uni {
		if b != pat(11, i) {
			t.Fatalf("contiñuación.dat[%d] mismatch", i)
		}
	}
	var f File
	if err := fsys.OpenFile(&f, "block.dat", ModeRead); err == nil {
		t.Fatal("block.dat still exists after Remove")
	}
	refill := readAllFile(t, &fsys, "refill.dat")
	if len(refill) != 15000 || refill[14999] != pat(14, 14999) {
		t.Fatalf("refill.dat mismatch (len %d)", len(refill))
	}
	if got := readAllFile(t, &fsys, "deep/deeper/deepest/bottom.txt"); string(got) != "made it" {
		t.Fatalf("bottom.txt = %q", got)
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("verify Unmount: %v", err)
	}
}

// TestExFATGoldenMount mounts the freshly formatted exFAT golden image and
// checks the root directory is empty.
func TestExFATGoldenMount(t *testing.T) {
	skipIfNoExFAT(t)
	dev := goldenDevice(t, "golden-fmtex.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	var root Dir
	if err := fsys.OpenDir(&root, "/"); err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	n := 0
	err := root.ForEachFile(func(fi *FileInfo) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachFile: %v", err)
	}
	if n != 0 {
		t.Fatalf("fresh exFAT root has %d entries, want 0", n)
	}
}

// TestExFATGoldenRead mounts the exFAT torture image produced by the C FatFs
// and verifies directory walking, name lookup (incl. unicode name hash) and
// file contents across contiguous and fragmented (FAT-chained) files.
func TestExFATGoldenRead(t *testing.T) {
	skipIfNoExFAT(t)
	dev := goldenDevice(t, "golden-tortureex.img")
	var fsys FS
	if err := fsys.Mount(dev, 512, ModeRead); err != nil {
		t.Fatalf("Mount: %v", err)
	}

	// Sizes recorded by the entry set (64-bit) must match the script state.
	var info FileInfo
	if err := fsys.Stat("big2.dat", &info); err != nil {
		t.Fatalf("Stat big2.dat: %v", err)
	}
	if info.Size() != 150001 {
		t.Errorf("big2.dat size = %d, want 150001", info.Size())
	}
	if err := fsys.Stat("deep/deeper/deepest", &info); err != nil {
		t.Fatalf("Stat deep dir: %v", err)
	}
	if !info.IsDir() {
		t.Error("deep/deeper/deepest is not a directory")
	}

	// String content through nested dot-entry-free directories.
	if got := string(readAllFile(t, &fsys, "deep/deeper/deepest/bottom.txt")); got != "made it" {
		t.Errorf("bottom.txt = %q, want %q", got, "made it")
	}
	if got := string(readAllFile(t, &fsys, "inner/done.txt")); got != "ok" {
		t.Errorf("done.txt = %q, want %q", got, "ok")
	}

	// refill.dat threads through bitmap holes: 15000 bytes of tag-14 pattern.
	refill := readAllFile(t, &fsys, "refill.dat")
	if len(refill) != 15000 {
		t.Fatalf("refill.dat len = %d, want 15000", len(refill))
	}
	for i, b := range refill {
		if b != pat(14, i) {
			t.Fatalf("refill.dat[%d] = %#02x, want %#02x", i, b, pat(14, i))
		}
	}

	// Unicode name: found via exFAT name hash + up-cased UTF-16 compare.
	// Truncated from a fragmented state back to a 5000 byte head.
	uni := readAllFile(t, &fsys, "deep/contiñuación.dat")
	if len(uni) != 5000 {
		t.Fatalf("contiñuación.dat len = %d, want 5000", len(uni))
	}
	for i, b := range uni {
		if b != pat(11, i) {
			t.Fatalf("contiñuación.dat[%d] = %#02x, want %#02x", i, b, pat(11, i))
		}
	}

	// big2.dat spot checks: tag-5 base pattern with a tag-6 WriteAt overlay
	// at 123456..127455 (truncated to 150001 bytes).
	var f File
	if err := fsys.OpenFile(&f, "big2.dat", ModeRead); err != nil {
		t.Fatalf("open big2.dat: %v", err)
	}
	checkPatAt(t, &f, "big2.dat", 5, 0, 2000, 0)
	checkPatAt(t, &f, "big2.dat", 6, 123456, 4000, 0)
	checkPatAt(t, &f, "big2.dat", 5, 130000, 2000, 130000)
	if err := f.Close(); err != nil {
		t.Fatalf("close big2.dat: %v", err)
	}

	// Root directory walk: check a couple of known names show up with
	// correct metadata.
	var root Dir
	if err := fsys.OpenDir(&root, "/"); err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	found := make(map[string]int64)
	err := root.ForEachFile(func(fi *FileInfo) error {
		switch fi.Name() {
		case "big2.dat", "refill.dat", "deep", "logs", "inner":
			found[fi.Name()] = fi.Size()
		}
		if fi.AlternateName() != "" {
			t.Errorf("exFAT file %q has altname %q, want empty", fi.Name(), fi.AlternateName())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachFile: %v", err)
	}
	if len(found) != 5 {
		t.Fatalf("root walk found %v, want 5 known entries", found)
	}
	if found["big2.dat"] != 150001 {
		t.Errorf("walk big2.dat size = %d, want 150001", found["big2.dat"])
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
}
