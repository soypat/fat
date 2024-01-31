package fat

import "errors"

type Format uint8

const (
	_FormatUnknown Format = iota
	FormatFAT12
	FormatFAT16
	FormatFAT32
	FormatExFAT
)

type Formatter struct {
	window     []byte
	windowaddr lba
	// block device is temporarily used by the formatter to read/write blocks.
	bd BlockDevice
}

type FormatConfig struct {
	Label string
	// ClusterSize is the size of a FAT cluster in blocks.
	ClusterSize int
	// Format selects the FAT format to use. If not specified will use FAT32.
	Format Format
	// Number of reserved blocks for FAT tables. Either 1 or 2. 0 defaults to 2.
	// NumberOfFATs uint8
}

func (f *Formatter) Format(bd BlockDevice, blocksize, fsSizeInBlocks int, cfg FormatConfig) error {
	if cfg.Format == 0 {
		cfg.Format = FormatFAT32
	}
	if blocksize <= 512 || fsSizeInBlocks <= 32 || bd == nil || cfg.Format != FormatFAT32 {
		return errors.New("invalid Format argument")
	}
	if len(f.window) < blocksize {
		f.window = make([]byte, blocksize)
	}
	if cfg.Label == "" {
		cfg.Label = "tinygo.unnamed"
	}
	f.windowaddr = ^lba(0)
	f.bd = bd

	switch cfg.Format {
	case FormatFAT12, FormatFAT16, FormatFAT32:
		return f.formatFAT(bd, blocksize, fsSizeInBlocks, cfg)
	case FormatExFAT:
		return frUnsupported
	default:
		return frUnsupported
	}
}

func (f *Formatter) formatFAT(bd BlockDevice, blocksize, fsSizeInBlocks int, cfg FormatConfig) error {
	const (
		fmAny byte = 1 << 7
		fmSFD byte = 1 << 6
	)
	// var (
	// 	sz_blk       = blocksize
	// 	ss           = blocksize
	// 	fsopt        = fmAny | fmSFD
	// 	n_fat  uint8 = 2
	// 	sz_au        = cfg.ClusterSize
	// )
	// if !(sz_au <= 0x1000000 && sz_au&(sz_au-1) == 0) {
	// 	return errors.New("invalid cluster size")
	// }

	// for {
	// 	pau := sz
	// }
	return nil
}

func (f *Formatter) move_window(addr lba) error {
	if addr != f.windowaddr {
		if _, err := f.bd.ReadBlocks(f.window, int64(addr)); err != nil {
			return err
		}
		f.windowaddr = addr
	}
	return nil
}
