package fat

import (
	"bytes"
	"testing"
)

// formatAndMount formats a fresh in-memory volume of numBlocks 512-byte blocks
// and mounts it read-write.
func formatAndMount(t *testing.T, numBlocks int, cfg FormatParams) (*FS, *BlockByteSlice) {
	t.Helper()
	blk, err := makeBlockIndexer(512)
	if err != nil {
		t.Fatal(err)
	}
	dev := &BlockByteSlice{blk: blk, buf: make([]byte, numBlocks*512)}
	var fmtr Formatter
	if err = fmtr.Format(dev, 512, numBlocks, cfg); err != nil {
		t.Fatal("format:", err)
	}
	var fsys FS
	if err = fsys.Mount(dev, 512, ModeRW); err != nil {
		t.Fatal("mount:", err)
	}
	return &fsys, dev
}

// TestFormatFATSubtype is the test that makes the FAT12/16/32 formatter worth
// having, and the one that a formatter is most likely to fail silently.
//
// Nothing in a boot record declares the FAT sub-type. A driver counts the
// clusters and decides — over 65525 it is FAT32, over 4085 it is FAT16, below
// that FAT12 — and it will happily mount a volume whose BS_FilSysType string says
// "FAT32   " as FAT16 if the cluster count says so. So a formatter that gets the
// geometry wrong produces something that formats, mounts, passes a round-trip
// test and is simply not the filesystem that was asked for.
//
// This asserts on fsys.fstype, which is the driver's own verdict, rather than on
// anything the formatter wrote about itself.
func TestFormatFATSubtype(t *testing.T) {
	for _, test := range []struct {
		name   string
		blocks int
		cfg    FormatParams
		want   Format
	}{
		// Auto cluster size throughout: it is the path a caller takes by default,
		// and on FAT32 it is the path that exercises the retry loop, which halves
		// the cluster size when the first choice leaves too few clusters to be
		// FAT32 at all.
		{"fat12", 4096, FormatParams{Format: FormatFAT12}, FormatFAT12},   // 2 MiB.
		{"fat16", 32768, FormatParams{Format: FormatFAT16}, FormatFAT16},  // 16 MiB.
		{"fat32", 131072, FormatParams{Format: FormatFAT32}, FormatFAT32}, // 64 MiB.

		// The floor. FAT32 needs more than 65525 clusters, so at one sector per
		// cluster — the smallest there is — the volume still cannot be under 32.5
		// MiB. 66600 sectors is the smallest FAT32 this formatter will produce:
		// f_mkfs sizes the FAT from szVol/pau, which counts the reserved sectors and
		// the FATs themselves as if they were clusters, so the FAT comes out a
		// little fatter than it strictly needs to be and the floor sits slightly
		// above the theoretical 65526 clusters. One sector less and this aborts.
		{"fat32 smallest", 66600, FormatParams{Format: FormatFAT32, ClusterSize: 1}, FormatFAT32},

		// A default FormatConfig means FAT32 (see Format), so it must behave the
		// same as asking for it.
		{"fat32 by default", 131072, FormatParams{}, FormatFAT32},
	} {
		t.Run(test.name, func(t *testing.T) {
			fsys, _ := formatAndMount(t, test.blocks, test.cfg)
			if fsys.fstype != test.want {
				t.Fatalf("mounted as fstype %d, want %d: the cluster count and the requested "+
					"sub-type disagree, so this volume is not the filesystem it was asked to be",
					fsys.fstype, test.want)
			}

			// And it has to work, not merely mount.
			const contents = "the quick brown fox"
			var fp File
			if err := fsys.OpenFile(&fp, "hello.txt", ModeWrite|ModeCreateAlways); err != nil {
				t.Fatal("create:", err)
			}
			if _, err := fp.Write([]byte(contents)); err != nil {
				t.Fatal("write:", err)
			}
			if err := fp.Close(); err != nil {
				t.Fatal("close:", err)
			}

			if err := fsys.OpenFile(&fp, "hello.txt", ModeRead); err != nil {
				t.Fatal("reopen:", err)
			}
			got := make([]byte, len(contents))
			if _, err := fp.Read(got); err != nil {
				t.Fatal("read:", err)
			}
			if err := fp.Close(); err != nil {
				t.Fatal("close:", err)
			}
			if !bytes.Equal(got, []byte(contents)) {
				t.Fatalf("read back %q, want %q", got, contents)
			}
		})
	}
}

// TestFormatFATTooSmallForFAT32 pins the refusal.
//
// A 16 MiB volume cannot be FAT32 at any cluster size: one sector per cluster is
// the smallest cluster there is, and that still only gives ~32k clusters, half of
// what FAT32 requires. The formatter must say so rather than write a volume that
// every driver will mount as FAT16 while its boot record insists it is FAT32.
func TestFormatFATTooSmallForFAT32(t *testing.T) {
	blk, err := makeBlockIndexer(512)
	if err != nil {
		t.Fatal(err)
	}
	const blocks = 32768 // 16 MiB.
	dev := &BlockByteSlice{blk: blk, buf: make([]byte, blocks*512)}
	var fmtr Formatter
	err = fmtr.Format(dev, 512, blocks, FormatParams{Format: FormatFAT32, ClusterSize: 1})
	if err != frMkfsAborted {
		t.Fatalf("formatting a 16 MiB volume as FAT32 returned %v, want frMkfsAborted: "+
			"FAT32 requires more than 65525 clusters and this volume cannot have them", err)
	}
}

// TestFormatFATSmallestFAT32IsExact pins the floor from below. A volume one
// sector smaller than the smallest FAT32 must be refused, not rounded down into
// a FAT16 that claims otherwise.
func TestFormatFATSmallestFAT32IsExact(t *testing.T) {
	const smallest = 66600
	blk, err := makeBlockIndexer(512)
	if err != nil {
		t.Fatal(err)
	}
	dev := &BlockByteSlice{blk: blk, buf: make([]byte, smallest*512)}
	var fmtr Formatter
	err = fmtr.Format(dev, 512, smallest-1, FormatParams{Format: FormatFAT32, ClusterSize: 1})
	if err != frMkfsAborted {
		t.Fatalf("formatting %d sectors as FAT32 returned %v, want frMkfsAborted: that is one "+
			"sector below the smallest volume with more than 65525 clusters", smallest-1, err)
	}
}

// TestFormatFATClusterSizeRetry pins the loop in formatFAT that adjusts the
// cluster size when the caller left it to us.
//
// A 64 MiB volume auto-selects 2 sectors per cluster, which yields about 65k
// clusters — just under the FAT32 floor. The formatter has to notice, halve the
// cluster size to 1, and try again, rather than abort or emit a FAT16 wearing a
// FAT32 boot record.
func TestFormatFATClusterSizeRetry(t *testing.T) {
	fsys, dev := formatAndMount(t, 131072, FormatParams{Format: FormatFAT32})
	if fsys.fstype != FormatFAT32 {
		t.Fatalf("mounted as fstype %d, want FAT32: the cluster size retry did not happen", fsys.fstype)
	}
	if got := dev.buf[bpbSecPerClus]; got != 1 {
		t.Errorf("cluster size is %d sectors, want 1: auto-selection picked 2, which leaves "+
			"too few clusters for FAT32, so the retry must have halved it", got)
	}
}
