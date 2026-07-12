package fat

import (
	"encoding/binary"
	"testing"
	"unicode/utf16"
)

// writeLabelEntry sets the volume label of a mounted volume by writing the
// root directory entry by hand. Format does not write a label entry — neither
// does FatFs' f_mkfs, and the golden images depend on it not doing so — so this
// is the only way to get a labelled volume here.
func writeLabelEntry(t *testing.T, fsys *FS, label string) {
	t.Helper()
	var dj dir
	dj.obj.fs = fsys
	dj.obj.sclust = 0 // Root directory.
	if fr := dj.sdi(0); fr != frOK {
		t.Fatal("sdi:", fr)
	}
	if fr := fsys.move_window(dj.sect); fr != frOK {
		t.Fatal("move_window:", fr)
	}
	if fsys.isExfat() {
		// The exFAT formatter leaves an empty 0x83 entry at the head of the
		// root directory, so the label goes in there.
		if dj.dir[xdirType] != etVLABEL {
			t.Fatalf("root directory entry 0 is %#x, want a volume label entry", dj.dir[xdirType])
		}
		u16 := utf16.Encode([]rune(label))
		if len(u16) > 11 {
			t.Fatal("exFAT label too long")
		}
		dj.dir[xdirNumLabel] = byte(len(u16))
		for i, wc := range u16 {
			binary.LittleEndian.PutUint16(dj.dir[xdirLabel+i*2:], wc)
		}
	} else {
		if len(label) > 11 {
			t.Fatal("FAT label too long")
		}
		copy(dj.dir[:11], "           ") // Space padded.
		copy(dj.dir[:11], label)
		dj.dir[dirAttrOff] = amVOL
	}
	fsys.wflag = 1
	if fr := fsys.sync(); fr != frOK {
		t.Fatal("sync:", fr)
	}
}

func TestLabel(t *testing.T) {
	for _, test := range []struct {
		name   string
		blocks int
		format Format
		label  string
		want   string
	}{
		{name: "fat12", blocks: 4096, format: FormatFAT12, label: "KEYLARGO", want: "KEYLARGO"},
		{name: "fat16", blocks: 32768, format: FormatFAT16, label: "KEYLARGO", want: "KEYLARGO"},
		{name: "fat32", blocks: 131072, format: FormatFAT32, label: "KEYLARGO", want: "KEYLARGO"},
		// The label is stored space padded to 11 bytes; the padding is not part of it.
		{name: "fat16 padded", blocks: 32768, format: FormatFAT16, label: "NO NAME", want: "NO NAME"},
		{name: "fat16 full", blocks: 32768, format: FormatFAT16, label: "ELEVENCHARS", want: "ELEVENCHARS"},
		// exFAT keeps the label as UTF-16 and does not fold case.
		{name: "exfat", blocks: 131072, format: FormatExFAT, label: "Key Largo", want: "Key Largo"},
		{name: "exfat unicode", blocks: 131072, format: FormatExFAT, label: "cañón—🔥", want: "cañón—🔥"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.format == FormatExFAT && !exfatEnabled {
				t.Skip("exFAT support excluded from build")
			}
			fsys, dev := formatAndMount(t, test.blocks, FormatParams{Format: test.format})

			// A freshly formatted volume has no label.
			got, err := fsys.AppendLabel(nil)
			if err != nil {
				t.Fatal("Label:", err)
			} else if string(got) != "" {
				t.Errorf("label of unlabelled volume = %q, want empty", got)
			}

			writeLabelEntry(t, fsys, test.label)
			got, err = fsys.AppendLabel(nil)
			if err != nil {
				t.Fatal("Label:", err)
			} else if string(got) != test.want {
				t.Errorf("Label = %q, want %q", got, test.want)
			}

			// The label survives a remount: it is read from the volume, not cached.
			if err = fsys.Unmount(); err != nil {
				t.Fatal("unmount:", err)
			}
			if _, err = fsys.AppendLabel(nil); err != frNoFilesystem {
				t.Errorf("Label of unmounted FS = %v, want %v", err, frNoFilesystem)
			}
			if err = fsys.Mount(dev, 512, ModeRW); err != nil {
				t.Fatal("remount:", err)
			}
			params, err := fsys.FormatParams()
			if err != nil {
				t.Fatal("FormatParams:", err)
			}
			if params.Label != test.want {
				t.Errorf("FormatParams().Label = %q, want %q", params.Label, test.want)
			}
			if params.Format != test.format {
				t.Errorf("FormatParams().Format = %d, want %d", params.Format, test.format)
			}
			if params.ClusterSize != int(fsys.csize) {
				t.Errorf("FormatParams().ClusterSize = %d, want %d", params.ClusterSize, fsys.csize)
			}
			if bs := fsys.BlockSize(); bs != 512 {
				t.Errorf("BlockSize = %d, want 512", bs)
			}
		})
	}
}

func TestAppendLabel(t *testing.T) {
	fsys, _ := formatAndMount(t, 32768, FormatParams{Format: FormatFAT16})
	writeLabelEntry(t, fsys, "KEYLARGO")
	dst := []byte("label=")
	dst, err := fsys.AppendLabel(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(dst) != "label=KEYLARGO" {
		t.Errorf("AppendLabel = %q, want %q", dst, "label=KEYLARGO")
	}
}
