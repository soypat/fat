package fat

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// This function is a self contained fuzzing function whose working
// principle is similiar to that of a virtual machine. It takes in
// a series of 64-bit operations and performs them on a FS object.
func FuzzFS(f *testing.F) {
	// 64-bit operation definition, starting with least significant bits:
	//
	//  - OP:       First 4 bits are the operation to perform.
	//  - WHO:      Next 4 bits is target of operation. A 0 value means random/nonexistent target.
	//  - PERM:     Next 2 bits are the permission, if applicable.
	//  - RESERVED: Middle bits are reserved.
	//  - DATASIZE: Last 16 bits is the size of the data to read/write, if applicable.
	const (
		opChangeDir uint64 = iota
		opCreateDir
		opCreateFile
		opOpenFile
		opReadFile
		opWriteFile
		opCloseFile

		datasizeOff = 48
		whoOff      = 4
	)
	type filinfo struct {
		file   File
		ptr    int64
		size   int64
		name   string
		closed bool
	}
	genName := func(fs *FS, dir string, who uint8) string {
		return dir + "/" + string('a'+who)
	}
	getWho := func(finfos []filinfo, who uint8) (filename *filinfo) {
		if len(finfos) == 0 {
			return nil
		}
		who %= uint8(len(finfos))
		return &finfos[who]
	}
	writeData := make([]byte, 1<<16)
	readData := make([]byte, 1<<16)
	for i := range writeData {
		writeData[i] = byte(i)
	}
	f.Add(opChangeDir, opCreateFile, opWriteFile|(1000<<datasizeOff),
		opCloseFile, opOpenFile, opReadFile|(1000<<datasizeOff),
		opChangeDir, opOpenFile|(1<<whoOff), opWriteFile|(1<<whoOff)|(1000<<datasizeOff),
		opCloseFile|(1<<whoOff), opOpenFile, opReadFile|(1<<whoOff)|(1001<<datasizeOff),
	)
	const totalFSSize = 2 * 32000
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
	f.Fuzz(func(t *testing.T, fsop0, fsop1, fsop2, fsop3, fsop4, fsop5, fsop6, fsop7, fsop8, fsop9, fsop10, fsop11 uint64) {
		fs, _ := initTestFATWithLogger(totalFSSize, logger)
		fsops := [...]uint64{fsop0, fsop1, fsop2, fsop3, fsop4, fsop5, fsop6, fsop7, fsop8, fsop9, fsop10, fsop11}
		fileinfos := make([]filinfo, 0, len(fsops))
		var dir string = "/"
		totalWritten := 0
		for _, fsop := range fsops {
			op := fsop & 0xf
			who := byte(fsop) >> 4
			perm := Mode(fsop>>8) & 3
			datasize := uint16(fsop >> 48)
			switch op {
			case opChangeDir:
				if dir == "/" {
					dir = "/rootdir"
				} else {
					dir = "/"
				}

			case opCreateFile:
				fileinfos = append(fileinfos, filinfo{})
				info := &fileinfos[len(fileinfos)-1]
				filename := genName(fs, dir, who)
				err := fs.OpenFile(&info.file, filename, perm|ModeCreateAlways)
				if err != nil {
					fileinfos = fileinfos[:len(fileinfos)-1] // Uncommit file on error.
				}
				info.name = filename

			case opOpenFile:
				info := getWho(fileinfos, who)
				if info == nil || !info.closed {
					// Don't open already open files for simplicity's sake.
					break
				}
				err := fs.OpenFile(&info.file, info.name, perm|ModeOpenExisting)
				if err == nil {
					info.closed = false
					info.ptr = 0
				}

			case opCloseFile:
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

			case opWriteFile:
				if totalWritten >= totalFSSize*4/5 {
					break // Avoid growing the filesystem too much.
				}
				info := getWho(fileinfos, who)
				if info == nil || info.closed {
					break
				}
				n, err := info.file.Write(writeData[:datasize])
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

			case opReadFile:
				info := getWho(fileinfos, who)
				if info == nil || info.closed {
					break
				}
				n, err := info.file.Read(readData[:datasize])
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
	})
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
