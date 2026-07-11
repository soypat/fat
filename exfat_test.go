package fat

import (
	"testing"
)

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
