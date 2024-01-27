package fat

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
)

func TestOpenRootFile(t *testing.T) {
	fs, _ := initTestFAT()
	var fp File
	fr := fs.f_open(&fp, "rootfile\x00", faRead)
	if fr != frOK {
		t.Fatal(fr.Error())
	}
}

func TestFileInfo(t *testing.T) {
	fs, _ := initTestFAT()
	var dir dir
	fr := fs.f_opendir(&dir, "rootdir\x00")
	if fr != frOK {
		t.Fatal(fr.Error())
	}
	var finfo fileinfo
	fr = dir.f_readdir(&finfo)
	if fr != frOK {
		t.Fatal(fr.Error())
	}
	t.Errorf("%+v", finfo)
}

func attachLogger(fs *FS) *slog.Logger {
	fs.log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slogLevelTrace,
	}))
	return fs.log
}

func ExampleRead() {
	const (
		filename = "test.txt\x00"
		data     = "abc123"
	)
	var fs FS
	log := attachLogger(&fs)
	dev := DefaultFATByteBlocks(32000)
	fr := fs.mount_volume(dev, uint16(dev.blk.size()), faRead|faWrite)
	if fr != frOK {
		log.Error("mount failed:" + fr.Error())
		return
	}

	var fp File

	fr = fs.f_open(&fp, filename, faRead|faWrite|faCreateNew)
	if fr != frOK {
		log.Error("open for write failed:" + fr.Error())
		return
	}

	n, fr := fp.f_write([]byte(data))
	if fr != frOK {
		log.Error("write failed:" + fr.Error())
		return
	}
	if n != len(data) {
		log.Error("write failed: short write")
		return
	}

	fr = fp.f_close()
	if fr != frOK {
		log.Error("close failed:" + fr.Error())
		return
	}

	// Read back data.
	fr = fs.f_open(&fp, filename, faRead)
	if fr != frOK {
		log.Error("open for read failed:" + fr.Error())
		return
	}
	buf := make([]byte, len(data))
	n, fr = fp.f_read(buf)
	if fr != frOK {
		log.Error("read failed:" + fr.Error())
		return
	}
	got := string(buf[:n])
	if got != data {
		log.Error("read failed", slog.String("got", got), slog.String("want", data))
		return
	}
	fr = fp.f_close()
	if fr != frOK {
		log.Error("close failed:" + fr.Error())
		return
	}
	fmt.Println("wrote and read back file OK!")
}

func DefaultFATByteBlocks(numBlocks int) *BytesBlocks {
	const defaultBlockSize = 512
	blk, _ := makeBlockIndexer(defaultBlockSize)
	buf := make([]byte, defaultBlockSize*numBlocks)
	for i, b := range fatInit {
		off := i * defaultBlockSize
		if off >= int64(len(buf)) {
			panic(fmt.Sprintf("fatInit[%d] out of range", i))
		}
		copy(buf[off:], b[:])
	}
	return &BytesBlocks{
		blk: blk,
		buf: buf,
	}
}

type BytesBlocks struct {
	blk blkIdxer
	buf []byte
}

func (b *BytesBlocks) ReadBlocks(dst []byte, startBlock int64) (int, error) {
	if b.blk.off(int64(len(dst))) != 0 {
		return 0, errors.New("startBlock not aligned to block size")
	} else if startBlock < 0 {
		return 0, errors.New("invalid startBlock")
	}
	off := startBlock * b.blk.size()
	end := off + int64(len(dst))
	if end > int64(len(b.buf)) {
		return 0, fmt.Errorf("read past end of buffer: %d > %d", end, len(b.buf))
		// return 0, errors.New("read past end of buffer")
	}

	return copy(dst, b.buf[off:end]), nil
}
func (b *BytesBlocks) WriteBlocks(data []byte, startBlock int64) (int, error) {
	if b.blk.off(int64(len(data))) != 0 {
		return 0, errors.New("startBlock not aligned to block size")
	} else if startBlock < 0 {
		return 0, errors.New("invalid startBlock")
	}
	off := startBlock * b.blk.size()
	end := off + int64(len(data))
	if end > int64(len(b.buf)) {
		return 0, fmt.Errorf("write past end of buffer: %d > %d", end, len(b.buf))
		// return 0, errors.New("write past end of buffer")
	}

	return copy(b.buf[off:end], data), nil
}
func (b *BytesBlocks) EraseSectors(startBlock, numBlocks int64) error {
	if startBlock < 0 || numBlocks <= 0 {
		return errors.New("invalid erase parameters")
	}
	start := startBlock * b.blk.size()
	end := start + numBlocks*b.blk.size()
	if end > int64(len(b.buf)) {
		return errors.New("erase past end of buffer")
	}
	clear(b.buf[start:end])
	return nil
}

func (b *BytesBlocks) Size() int64 {
	return int64(len(b.buf))
}

// Mode returns 0 for no connection/prohibited access, 1 for read-only, 3 for read-write.
func (b *BytesBlocks) Mode() uint8 {
	return 3
}

func fatInitDiff(data []byte) (s string) {
	max := int64(len(data)) / 512
	for block := int64(0); block < max; block++ {
		b := data[block*512 : (block+1)*512]
		expect := fatInit[block]
		if !bytes.Equal(b, expect[:]) {
			s += fmt.Sprintf("block %d got!=want:\n%s\n%s\n", block, hex.Dump(b), hex.Dump(expect[:]))
		}
	}
	if len(s) == 0 {
		return "no differences"
	}
	return s
}

func initTestFAT() (*FS, *BytesBlocks) {
	dev := DefaultFATByteBlocks(32000)
	var fs FS
	attachLogger(&fs)
	ss := uint16(dev.blk.size())
	fr := fs.mount_volume(dev, ss, faRead|faWrite)
	if fr != frOK {
		panic(fr.Error())
	}
	return &fs, dev
}

// Start of clean slate FAT32 filesystem image with name `keylargo`, 8GB in size.
// Contains a folder structure with a rootfile with some test, a rootdir directory
// with a file in it.
var fatInit = map[int64][512]byte{
	// Boot sector.
	0: {0xeb, 0x58, 0x90, 0x6d, 0x6b, 0x66, 0x73, 0x2e, 0x66, 0x61, 0x74, 0x00, 0x02, 0x08, 0x20, 0x00, // |.X.mkfs.fat... .|
		0x02, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x00, 0x00, 0x3e, 0x00, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, // |........>.......|
		0xd0, 0x07, 0xf0, 0x00, 0xe8, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // |.....;..........|
		0x01, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // |................|
		0x80, 0x00, 0x29, 0x06, 0xf1, 0x12, 0xc5, 0x6b, 0x65, 0x79, 0x6c, 0x61, 0x72, 0x67, 0x6f, 0x20, // |..)....keylargo |
		0x20, 0x20, 0x46, 0x41, 0x54, 0x33, 0x32, 0x20, 0x20, 0x20, 0x0e, 0x1f, 0xbe, 0x77, 0x7c, 0xac, // |  FAT32   ...w|.|
		0x22, 0xc0, 0x74, 0x0b, 0x56, 0xb4, 0x0e, 0xbb, 0x07, 0x00, 0xcd, 0x10, 0x5e, 0xeb, 0xf0, 0x32, // |".t.V.......^..2|
		0xe4, 0xcd, 0x16, 0xcd, 0x19, 0xeb, 0xfe, 0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x6e, // |.......This is n|
		0x6f, 0x74, 0x20, 0x61, 0x20, 0x62, 0x6f, 0x6f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x64, 0x69, // |ot a bootable di|
		0x73, 0x6b, 0x2e, 0x20, 0x20, 0x50, 0x6c, 0x65, 0x61, 0x73, 0x65, 0x20, 0x69, 0x6e, 0x73, 0x65, // |sk.  Please inse|
		0x72, 0x74, 0x20, 0x61, 0x20, 0x62, 0x6f, 0x6f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x66, 0x6c, // |rt a bootable fl|
		0x6f, 0x70, 0x70, 0x79, 0x20, 0x61, 0x6e, 0x64, 0x0d, 0x0a, 0x70, 0x72, 0x65, 0x73, 0x73, 0x20, // |oppy and..press |
		0x61, 0x6e, 0x79, 0x20, 0x6b, 0x65, 0x79, 0x20, 0x74, 0x6f, 0x20, 0x74, 0x72, 0x79, 0x20, 0x61, // |any key to try a|
		0x67, 0x61, 0x69, 0x6e, 0x20, 0x2e, 0x2e, 0x2e, 0x20, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, // |gain ... .......|
		510: 0x55, 0xaa,
	},

	1: {0: 0x52, 0x52, 0x61, 0x41,
		484: 0x72, 0x72, 0x41, 0x61, 0xf8, 0xf1, 0x1d, 0x00, 0x05,
		510: 0x55, 0xaa},

	6: {0xeb, 0x58, 0x90, 0x6d, 0x6b, 0x66, 0x73, 0x2e, 0x66, 0x61, 0x74, 0x00, 0x02, 0x08, 0x20, 0x00, // |.X.mkfs.fat... .|
		0x02, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x00, 0x00, 0x3e, 0x00, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, // |........>.......|
		0xd0, 0x07, 0xf0, 0x00, 0xe8, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // |.....;..........|
		0x01, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // |................|
		0x80, 0x00, 0x29, 0x06, 0xf1, 0x12, 0xc5, 0x6b, 0x65, 0x79, 0x6c, 0x61, 0x72, 0x67, 0x6f, 0x20, // |..)....keylargo |
		0x20, 0x20, 0x46, 0x41, 0x54, 0x33, 0x32, 0x20, 0x20, 0x20, 0x0e, 0x1f, 0xbe, 0x77, 0x7c, 0xac, // |  FAT32   ...w|.|
		0x22, 0xc0, 0x74, 0x0b, 0x56, 0xb4, 0x0e, 0xbb, 0x07, 0x00, 0xcd, 0x10, 0x5e, 0xeb, 0xf0, 0x32, // |".t.V.......^..2|
		0xe4, 0xcd, 0x16, 0xcd, 0x19, 0xeb, 0xfe, 0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x6e, // |.......This is n|
		0x6f, 0x74, 0x20, 0x61, 0x20, 0x62, 0x6f, 0x6f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x64, 0x69, // |ot a bootable di|
		0x73, 0x6b, 0x2e, 0x20, 0x20, 0x50, 0x6c, 0x65, 0x61, 0x73, 0x65, 0x20, 0x69, 0x6e, 0x73, 0x65, // |sk.  Please inse|
		0x72, 0x74, 0x20, 0x61, 0x20, 0x62, 0x6f, 0x6f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x66, 0x6c, // |rt a bootable fl|
		0x6f, 0x70, 0x70, 0x79, 0x20, 0x61, 0x6e, 0x64, 0x0d, 0x0a, 0x70, 0x72, 0x65, 0x73, 0x73, 0x20, // |oppy and..press |
		0x61, 0x6e, 0x79, 0x20, 0x6b, 0x65, 0x79, 0x20, 0x74, 0x6f, 0x20, 0x74, 0x72, 0x79, 0x20, 0x61, // |any key to try a|
		0x67, 0x61, 0x69, 0x6e, 0x20, 0x2e, 0x2e, 0x2e, 0x20, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, // |gain ... .......|
		510: 0x55, 0xaa,
	},

	7: {0: 0x52, 0x52, 0x61, 0x41,
		484: 0x72, 0x72, 0x41, 0x61, 0xfb, 0xf1, 0x1d, 0x00, 0x02,
		510: 0x55, 0xaa},

	32: {0xf8, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0x0f, 0xf8, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0x0f,
		0xff, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0x0f},
	15368: {0xf8, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0x0f, 0xf8, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0x0f,
		0xff, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0x0f},

	30704: { // Root directory contents.
		0x6b, 0x65, 0x79, 0x6c, 0x61, 0x72, 0x67, 0x6f, 0x20, 0x20, 0x20, 0x08, 0x00, 0x00, 0xba, 0x53, // |keylargo   ....S|
		0x35, 0x58, 0x35, 0x58, 0x00, 0x00, 0xba, 0x53, 0x35, 0x58, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // |5X5X...S5X......|
		0x41, 0x72, 0x00, 0x6f, 0x00, 0x6f, 0x00, 0x74, 0x00, 0x66, 0x00, 0x0f, 0x00, 0x1a, 0x69, 0x00, // |Ar.o.o.t.f....i.|
		0x6c, 0x00, 0x65, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |l.e.............|
		0x52, 0x4f, 0x4f, 0x54, 0x46, 0x49, 0x4c, 0x45, 0x20, 0x20, 0x20, 0x20, 0x00, 0x03, 0xd4, 0xbb, // |ROOTFILE    ....|
		0x37, 0x58, 0x37, 0x58, 0x00, 0x00, 0xd4, 0xbb, 0x37, 0x58, 0x04, 0x00, 0x16, 0x00, 0x00, 0x00, // |7X7X....7X......|
		0x41, 0x72, 0x00, 0x6f, 0x00, 0x6f, 0x00, 0x74, 0x00, 0x64, 0x00, 0x0f, 0x00, 0xde, 0x69, 0x00, // |Ar.o.o.t.d....i.|
		0x72, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |r...............|
		0x52, 0x4f, 0x4f, 0x54, 0x44, 0x49, 0x52, 0x20, 0x20, 0x20, 0x20, 0x10, 0x00, 0x29, 0xe4, 0xbb, // |ROOTDIR    ..)..|
		0x37, 0x58, 0x37, 0x58, 0x00, 0x00, 0xe4, 0xbb, 0x37, 0x58, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, // |7X7X....7X......|
		0xe5, 0x6d, 0x00, 0x2d, 0x00, 0x4e, 0x00, 0x44, 0x00, 0x38, 0x00, 0x0f, 0x00, 0x95, 0x4a, 0x00, // |.m.-.N.D.8....J.|
		0x49, 0x00, 0x32, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |I.2.............|
		0xe5, 0x2e, 0x00, 0x67, 0x00, 0x6f, 0x00, 0x75, 0x00, 0x74, 0x00, 0x0f, 0x00, 0x95, 0x70, 0x00, // |...g.o.u.t....p.|
		0x75, 0x00, 0x74, 0x00, 0x73, 0x00, 0x74, 0x00, 0x72, 0x00, 0x00, 0x00, 0x65, 0x00, 0x61, 0x00, // |u.t.s.t.r...e.a.|
		0xe5, 0x4f, 0x55, 0x54, 0x50, 0x55, 0x7e, 0x31, 0x20, 0x20, 0x20, 0x20, 0x00, 0x03, 0xd4, 0xbb, // |.OUTPU~1    ....|
		0x37, 0x58, 0x37, 0x58, 0x00, 0x00, 0xd4, 0xbb, 0x37, 0x58, 0x04, 0x00, 0x16, 0x00, 0x00, 0x00, // |7X7X....7X......|
	},

	30712: { // Root subdirectory "rootdir" contents.
		0x2e, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x10, 0x00, 0x28, 0x64, 0xb6, // |.          ..(d.|
		0x37, 0x58, 0x37, 0x58, 0x00, 0x00, 0x64, 0xb6, 0x37, 0x58, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, // |7X7X..d.7X......|
		0x2e, 0x2e, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x10, 0x00, 0x28, 0x64, 0xb6, // |..         ..(d.|
		0x37, 0x58, 0x37, 0x58, 0x00, 0x00, 0x64, 0xb6, 0x37, 0x58, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // |7X7X..d.7X......|
		0x41, 0x64, 0x00, 0x69, 0x00, 0x72, 0x00, 0x66, 0x00, 0x69, 0x00, 0x0f, 0x00, 0x27, 0x6c, 0x00, // |Ad.i.r.f.i...'l.|
		0x65, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |e...............|
		0x44, 0x49, 0x52, 0x46, 0x49, 0x4c, 0x45, 0x20, 0x20, 0x20, 0x20, 0x20, 0x00, 0x28, 0xe4, 0xbb, // |DIRFILE     .(..|
		0x37, 0x58, 0x37, 0x58, 0x00, 0x00, 0xe4, 0xbb, 0x37, 0x58, 0x05, 0x00, 0x49, 0x00, 0x00, 0x00, // |7X7X....7X..I...|
		0xe5, 0x6d, 0x00, 0x2d, 0x00, 0x48, 0x00, 0x49, 0x00, 0x47, 0x00, 0x0f, 0x00, 0x95, 0x37, 0x00, // |.m.-.H.I.G....7.|
		0x48, 0x00, 0x32, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |H.2.............|
		0xe5, 0x2e, 0x00, 0x67, 0x00, 0x6f, 0x00, 0x75, 0x00, 0x74, 0x00, 0x0f, 0x00, 0x95, 0x70, 0x00, // |...g.o.u.t....p.|
		0x75, 0x00, 0x74, 0x00, 0x73, 0x00, 0x74, 0x00, 0x72, 0x00, 0x00, 0x00, 0x65, 0x00, 0x61, 0x00, // |u.t.s.t.r...e.a.|
		0xe5, 0x4f, 0x55, 0x54, 0x50, 0x55, 0x7e, 0x31, 0x20, 0x20, 0x20, 0x20, 0x00, 0x28, 0xe4, 0xbb, // |.OUTPU~1    .(..|
		0x37, 0x58, 0x37, 0x58, 0x00, 0x00, 0xe4, 0xbb, 0x37, 0x58, 0x05, 0x00, 0x49, 0x00, 0x00, 0x00, // |7X7X....7X..I...|
	},

	// Below are file contents.

	// Says: "This is\nthe rootfile"
	30720: {0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x0a, 0x74, 0x68, 0x65, 0x20, 0x72, 0x6f, 0x6f, 0x74, 0x20, 0x66, 0x69, 0x6c, 0x65, 0x0a},
	30728: {
		0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x6e, 0x6f, 0x74, 0x0a, 0x6e, 0x6f, 0x74, 0x20, // |this is not.not |
		0x74, 0x68, 0x65, 0x20, 0x72, 0x6f, 0x6f, 0x74, 0x0a, 0x6e, 0x6f, 0x74, 0x20, 0x74, 0x68, 0x65, // |the roo7t.not the
		0x20, 0x72, 0x6f, 0x6f, 0x74, 0x20, 0x66, 0x69, 0x6c, 0x65, 0x0a, 0x6e, 0x6f, 0x70, 0x65, 0x2e, // | root file.nope.|
		0x20, 0x0a, 0x54, 0x68, 0x69, 0x73, 0x20, 0x66, 0x69, 0x6c, 0x65, 0x20, 0x68, 0x61, 0x73, 0x20, // | .This file has |
		0x35, 0x20, 0x6c, 0x69, 0x6e, 0x65, 0x73, 0x2e, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // |5 lines.........|
	},
}
