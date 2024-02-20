package fat

import (
	"errors"
	"fmt"
)

type BlockDeviceExtended interface {
	BlockDevice
	Size() int64
	BlockSize() int
}

const blkmapsize = 512

type BlockMap struct {
	data map[int64][blkmapsize]byte
}

func (b *BlockMap) BlockSize() int { return blkmapsize }

func (b *BlockMap) Size() int64 {
	const kilobyte = 1000
	const megabyte = 1000 * kilobyte
	const gigabyte = 1000 * megabyte
	return 4 * gigabyte // 4GB does not overflow uint32, so likely safe for use with FAT32?
}

func (b *BlockMap) ReadBlocks(dst []byte, startBlock int64) (int, error) {
	if startBlock < 0 {
		return 0, errors.New("invalid startBlock")
	}

	lastbidx := len(dst) / blkmapsize
	if len(dst)%blkmapsize != 0 {
		return 0, errors.New("dst size not multiple of block size")
	}
	for bidx := int64(0); bidx < int64(lastbidx); bidx++ {
		block := b.data[bidx]
		copy(dst[:], block[:])
		dst = dst[blkmapsize:]
	}
	return len(dst), nil
}

func (b *BlockMap) WriteBlocks(data []byte, startBlock int64) (int, error) {
	if startBlock < 0 {
		return 0, errors.New("invalid startBlock")
	}

	lastbidx := len(data) / blkmapsize
	if len(data)%blkmapsize != 0 {
		return 0, errors.New("data size not multiple of block size")
	}
	var auxblk [blkmapsize]byte
	for bidx := int64(0); bidx < int64(lastbidx); bidx++ {
		copy(auxblk[:], data[:])
		b.data[bidx] = auxblk
		data = data[blkmapsize:]
		auxblk = [blkmapsize]byte{}
	}
	return len(data), nil
}

func (b *BlockMap) EraseBlocks(startBlock, numBlocks int64) error {
	if startBlock < 0 || numBlocks <= 0 {
		return errors.New("invalid erase parameters")
	}
	end := startBlock + numBlocks
	if end < startBlock {
		return errors.New("overflow")
	}
	if len(b.data) > 1024 {
		// Optimized for maps with many entries.
		for i := startBlock; i < end; i++ {
			delete(b.data, i)
		}
	} else {
		// Optimized for maps with few entries.
		for blkidx := range b.data {
			if blkidx >= startBlock && blkidx < end {
				delete(b.data, blkidx)
			}
		}
	}
	return nil
}

type BlockByteSlice struct {
	blk blkIdxer
	buf []byte
}

func (b *BlockByteSlice) BlockSize() int { return int(b.blk.size()) }

func (b *BlockByteSlice) ReadBlocks(dst []byte, startBlock int64) (int, error) {
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
func (b *BlockByteSlice) WriteBlocks(data []byte, startBlock int64) (int, error) {
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
func (b *BlockByteSlice) EraseBlocks(startBlock, numBlocks int64) error {
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

func (b *BlockByteSlice) Size() int64 {
	return int64(len(b.buf))
}
