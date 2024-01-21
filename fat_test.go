package fat

import (
	"errors"
	"log"
)

func ExampleRead() {
	var fs FS
	dev := DefaultByteBlocks(4)
	fr := fs.mount_volume(dev, uint16(dev.Size()), faRead|faWrite)
	if fr != frOK {
		log.Fatal("mount failed:" + fr.Error())
	}

	//Output:

}

func DefaultByteBlocks(numBlocks int) *BytesBlocks {
	const defaultBlockSize = 512
	blk, _ := makeBlockIndexer(defaultBlockSize)
	return &BytesBlocks{
		blk: blk,
		buf: make([]byte, defaultBlockSize*numBlocks),
	}
}

type BytesBlocks struct {
	blk blkIdxer
	buf []byte
}

func (b *BytesBlocks) ReadBlocks(dst []byte, startBlock int64) error {
	if b.blk.off(int64(len(dst))) != 0 {
		return errors.New("startBlock not aligned to block size")
	} else if startBlock < 0 {
		return errors.New("invalid startBlock")
	}
	off := startBlock * b.blk.size()
	end := off + int64(len(dst))
	if end > int64(len(b.buf)) {
		return errors.New("read past end of buffer")
	}
	copy(dst, b.buf[off:end])
	return nil
}
func (b *BytesBlocks) WriteBlocks(data []byte, startBlock int64) error {
	if b.blk.off(int64(len(data))) != 0 {
		return errors.New("startBlock not aligned to block size")
	} else if startBlock < 0 {
		return errors.New("invalid startBlock")
	}
	off := startBlock * b.blk.size()
	end := off + int64(len(data))
	if end > int64(len(b.buf)) {
		return errors.New("write past end of buffer")
	}
	copy(b.buf[off:end], data)
	return nil
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
