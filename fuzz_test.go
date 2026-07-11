package fat

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// Fuzz VM op encoding, starting with least significant bits:
//
//   - OP:       First 4 bits are the operation to perform.
//   - WHO:      Next 4 bits is target of operation. A 0 value means random/nonexistent target.
//   - PERM:     Next 2 bits are the permission, if applicable.
//   - RESERVED: Middle bits are reserved.
//   - DATASIZE: Last 16 bits is the size of the data to read/write, if applicable.
const (
	fuzzOpChangeDir uint64 = iota
	fuzzOpCreateDir
	fuzzOpCreateFile
	fuzzOpOpenFile
	fuzzOpReadFile
	fuzzOpWriteFile
	fuzzOpCloseFile

	fuzzDatasizeOff = 48
	fuzzWhoOff      = 4
)

const fuzzTotalFSSize = 2 * 32000

type fuzzFilinfo struct {
	file   File
	ptr    int64
	size   int64
	name   string
	closed bool
}

var fuzzWriteData, fuzzReadData []byte

func init() {
	fuzzWriteData = make([]byte, 1<<16)
	fuzzReadData = make([]byte, 1<<16)
	for i := range fuzzWriteData {
		fuzzWriteData[i] = byte(i)
	}
}

// This function is a self contained fuzzing function whose working
// principle is similiar to that of a virtual machine. It takes in
// a series of 64-bit operations and performs them on a FS object.
// With exFAT support compiled in, the same op sequence also runs against
// a fresh copy of a pre-formatted exFAT volume.
func FuzzFS(f *testing.F) {
	const (
		opChangeDir  = fuzzOpChangeDir
		opCreateFile = fuzzOpCreateFile
		opOpenFile   = fuzzOpOpenFile
		opReadFile   = fuzzOpReadFile
		opWriteFile  = fuzzOpWriteFile
		opCloseFile  = fuzzOpCloseFile
		datasizeOff  = fuzzDatasizeOff
		whoOff       = fuzzWhoOff
	)
	f.Add(opChangeDir, opCreateFile, opWriteFile|(1000<<datasizeOff),
		opCloseFile, opOpenFile, opReadFile|(1000<<datasizeOff),
		opChangeDir, opOpenFile|(1<<whoOff), opWriteFile|(1<<whoOff)|(1000<<datasizeOff),
		opCloseFile|(1<<whoOff), opOpenFile, opReadFile|(1<<whoOff)|(1001<<datasizeOff),
	)
	// Multi-cluster writes (cluster size is 4096) followed by CreateAlways
	// truncation of the same file: exercises cluster chain allocation,
	// remove_chain and allocation-hole reuse.
	f.Add(opCreateFile|(3<<8), opWriteFile|(20000<<datasizeOff),
		opCloseFile, opCreateFile|(3<<8), opWriteFile|(5000<<datasizeOff),
		opCloseFile, opOpenFile|(1<<8), opReadFile|(5000<<datasizeOff),
		opReadFile|(1<<datasizeOff), opCloseFile, opOpenFile|(1<<8), opReadFile|(60000<<datasizeOff),
	)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
	// Pre-format an exFAT baseline once; each fuzz execution runs the same
	// op sequence against a fresh copy of it after the FAT32 run.
	var exfatBase []byte
	if exfatEnabled {
		_, dev, err := initTestExFAT(fuzzTotalFSSize)
		if err != nil {
			f.Fatal(err)
		}
		exfatBase = dev.buf
	}
	f.Fuzz(func(t *testing.T, fsop0, fsop1, fsop2, fsop3, fsop4, fsop5, fsop6, fsop7, fsop8, fsop9, fsop10, fsop11 uint64) {
		fsops := [...]uint64{fsop0, fsop1, fsop2, fsop3, fsop4, fsop5, fsop6, fsop7, fsop8, fsop9, fsop10, fsop11}
		fs, _ := initTestFATWithLogger(fuzzTotalFSSize, logger)
		runFuzzOps(fs, fsops[:])
		if exfatEnabled {
			blk, _ := makeBlockIndexer(512)
			buf := make([]byte, len(exfatBase))
			copy(buf, exfatBase)
			var exfs FS
			if err := exfs.Mount(&BlockByteSlice{blk: blk, buf: buf}, 512, ModeRW); err != nil {
				panic(err)
			}
			runFuzzOps(&exfs, fsops[:])
		}
	})
}

// runFuzzOps executes the fuzz op sequence against fs; any panic is a bug.
func runFuzzOps(fs *FS, fsops []uint64) {
	genName := func(dir string, who uint8) string {
		return dir + "/" + string('a'+who)
	}
	getWho := func(finfos []fuzzFilinfo, who uint8) *fuzzFilinfo {
		if len(finfos) == 0 {
			return nil
		}
		who %= uint8(len(finfos))
		return &finfos[who]
	}
	// isOpen reports whether any open handle references the named file.
	// Duplicate simultaneous opens of the same file are undefined behavior
	// in FatFs without file locking (FF_FS_LOCK), so the fuzz VM avoids them.
	isOpen := func(finfos []fuzzFilinfo, name string) bool {
		for i := range finfos {
			if !finfos[i].closed && finfos[i].name == name {
				return true
			}
		}
		return false
	}
	fileinfos := make([]fuzzFilinfo, 0, len(fsops))
	var dir string = "/"
	totalWritten := 0
	for _, fsop := range fsops {
		op := fsop & 0xf
		who := byte(fsop) >> 4
		perm := Mode(fsop>>8) & 3
		datasize := uint16(fsop >> 48)
		switch op {
		case fuzzOpChangeDir:
			if dir == "/" {
				dir = "/rootdir"
			} else {
				dir = "/"
			}

		case fuzzOpCreateFile:
			filename := genName(dir, who)
			if isOpen(fileinfos, filename) {
				break // Duplicate open is UB in FatFs without FF_FS_LOCK.
			}
			fileinfos = append(fileinfos, fuzzFilinfo{})
			info := &fileinfos[len(fileinfos)-1]
			err := fs.OpenFile(&info.file, filename, perm|ModeCreateAlways)
			if err != nil {
				fileinfos = fileinfos[:len(fileinfos)-1] // Uncommit file on error.
			}
			info.name = filename

		case fuzzOpOpenFile:
			info := getWho(fileinfos, who)
			if info == nil || !info.closed || isOpen(fileinfos, info.name) {
				// Don't open already open files: duplicate open is UB
				// in FatFs without FF_FS_LOCK.
				break
			}
			err := fs.OpenFile(&info.file, info.name, perm|ModeOpenExisting)
			if err == nil {
				info.closed = false
				info.ptr = 0
			}

		case fuzzOpCloseFile:
			info := getWho(fileinfos, who)
			if info == nil {
				break
			}
			err := info.file.Close()
			if err != nil && !info.closed {
				panic(err)
			}
			info.ptr = 0
			info.closed = true

		case fuzzOpWriteFile:
			if totalWritten >= fuzzTotalFSSize*4/5 {
				break // Avoid growing the filesystem too much.
			}
			info := getWho(fileinfos, who)
			if info == nil || info.closed {
				break
			}
			n, err := info.file.Write(fuzzWriteData[:datasize])
			if info.file.Mode()&ModeWrite == 0 {
				if n != 0 {
					panic("forbidden write")
				}
				break // Ignore if file not writable.
			}
			if err != nil {
				panic(err)
			} else if n != int(datasize) {
				panic("n != dsize")
			}
			info.ptr = min(info.ptr+int64(n), info.size)
			if info.ptr > info.size {
				info.size = info.ptr
			}
			totalWritten += n

		case fuzzOpReadFile:
			info := getWho(fileinfos, who)
			if info == nil || info.closed {
				break
			}
			n, err := info.file.Read(fuzzReadData[:datasize])
			if info.file.Mode()&ModeRead == 0 {
				if n != 0 {
					panic("forbidden read")
				}
				break // Ignore if file not readable.
			}
			if err != nil && err != io.EOF {
				panic(err)
			}
		}
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
