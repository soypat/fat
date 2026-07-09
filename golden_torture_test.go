package fat

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// goldenImage returns a BlockDevice backed by the named golden image file.
func goldenImage(t *testing.T, name string) BlockDeviceExtended {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read golden image: %v", err)
	}
	return &BlockByteSlice{buf: data}
}

// pat returns the deterministic byte at offset i for the given tag.
// Must match the C implementation in mkgolden.c.
func pat(tag, i int) byte {
	return byte(i*31 + tag*17 + 7)
}

// writePat writes a patterned region to f starting at file offset start.
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
			t.Fatalf("writePat: %v", err)
		}
		done += c
	}
}

// TestGoldenTorture replays a deterministic operation script using only the
// currently exported high-level API (OpenFile, Write, Close, Sync on File).
// The resulting image is compared byte-for-byte with the golden image
// produced by the companion C program testdata/mkgolden.c that drives the
// original FatFs ff16 library with the identical script.
func TestGoldenTorture(t *testing.T) {
	const imageSize = 8192 * 512 // matches NUM_SECTORS in mkgolden.c
	dev := &BlockByteSlice{buf: make([]byte, imageSize)}

	var fs FS
	if err := fs.Mount(dev, 512, ModeRW); err != nil {
		t.Fatalf("Mount: %v", err)
	}

	// 1. Create "a.dat" and write 3 clusters (tag 1).
	var fa File
	if err := fs.OpenFile(&fa, "a.dat", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatalf("create a.dat: %v", err)
	}
	writePat(t, &fa, 1, 0, 3*4096)
	if err := fa.Close(); err != nil {
		t.Fatalf("close a.dat: %v", err)
	}

	// 2. Create "b.dat" and write 2 clusters (tag 2).
	var fb File
	if err := fs.OpenFile(&fb, "b.dat", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatalf("create b.dat: %v", err)
	}
	writePat(t, &fb, 2, 0, 2*4096)
	if err := fb.Close(); err != nil {
		t.Fatalf("close b.dat: %v", err)
	}

	// 3. Extend "a.dat" by another 2 clusters (current offset after first write is 3*4096).
	if err := fs.OpenFile(&fa, "a.dat", ModeOpenExisting|ModeWrite); err != nil {
		t.Fatalf("open a.dat for extend: %v", err)
	}
	// Because Seek is not yet exported we simply continue writing from the
	// current file pointer (already at EOF after previous close+reopen).
	writePat(t, &fa, 1, 3*4096, 2*4096)
	if err := fa.Close(); err != nil {
		t.Fatalf("close a.dat after extend: %v", err)
	}

	// 4. Create a fragmented file "frag.dat" by writing, closing, re-opening and appending.
	var ff File
	if err := fs.OpenFile(&ff, "frag.dat", ModeCreateAlways|ModeWrite); err != nil {
		t.Fatalf("create frag.dat: %v", err)
	}
	writePat(t, &ff, 3, 0, 4096)
	if err := ff.Close(); err != nil {
		t.Fatalf("close frag.dat: %v", err)
	}
	if err := fs.OpenFile(&ff, "frag.dat", ModeOpenExisting|ModeWrite); err != nil {
		t.Fatalf("reopen frag.dat: %v", err)
	}
	// Again, write from current EOF to force allocator to pick a new cluster.
	writePat(t, &ff, 3, 4096, 4096)
	if err := ff.Close(); err != nil {
		t.Fatalf("close frag.dat after append: %v", err)
	}

	// 5. Create 32 small files to force directory growth.
	for i := 0; i < 32; i++ {
		name := "small" + strconv.Itoa(i) + ".txt"
		var f File
		if err := fs.OpenFile(&f, name, ModeCreateAlways|ModeWrite); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := f.Write([]byte("hello")); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close %s: %v", name, err)
		}
	}

	// 6. Sync filesystem (flushes FSInfo, etc.).
	if err := fs.Sync(); err != nil {
		t.Fatalf("fs.Sync: %v", err)
	}

	// Compare resulting image against golden file produced by mkgolden.c.
	golden := goldenImage(t, "golden-torture.img")
	if !bytes.Equal(dev.buf, golden.(*BlockByteSlice).buf) {
		t.Fatalf("golden image mismatch")
	}
}
