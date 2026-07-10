//go:build fat_nolfn

package fat

import (
	"encoding/binary"
	"strings"
)

// lfnbuffer is empty when long file name support is disabled with the
// fat_nolfn build tag (FatFs' FF_USE_LFN=0). Only 8.3 short names work.
type lfnbuffer = [0]uint16

// fnamebuffer holds a file's name in [FileInfo], sized to fit an 8.3 SFN.
type fnamebuffer = [sfnBufSize + 1]byte

// initNames prepares the filename handling state during mount.
func (fsys *FS) initNames() {
	// OEM code page 437 (U.S.). CP437 is single-byte: the DBCS range table is
	// set so that dbc_1st/dbc_2nd never match. No unicode conversion table is
	// referenced so it is excluded from the build.
	fsys.exCvt = _tblCT437[:]
	fsys.dbcTbl = [10]byte{0xFF}
}

// pick_lfn always fails without LFN support so that directory reads skip LFN entries.
func (fsys *FS) pick_lfn(dir []byte) bool { return false }

// cmp_lfn always fails without LFN support so that name search matches SFN entries only.
func (fsys *FS) cmp_lfn(dir []byte) bool { return false }

// put_lfn is unreachable without LFN support: register never allocates LFN entries.
func (fsys *FS) put_lfn(dir []byte, ord, sum byte) {}

func (fsys *FS) lfnlen() int { return 0 }

// gen_numname is unreachable without LFN support: create_name never sets nsLOSS.
func (fsys *FS) gen_numname(dst, src []byte, lfn []uint16, seq uint32) {}

// create_name converts a path segment directly into an 8.3 SFN in dp.fn.
// Names that do not fit the 8.3 format are rejected with frInvalidName
// instead of being lossy-converted as in the LFN build.
func (dp *dir) create_name(path string) (string, fileResult) {
	fsys := dp.obj.fs
	fsys.trace("dir:create_name")
	for i := 0; i < 11; i++ {
		dp.fn[i] = ' ' // memset(dp.fn, ' ', 11);
	}
	var (
		p  = path
		si = 0
		i  = 0
		ni = 8
	)
	for {
		var c byte
		if si < len(p) {
			c = p[si]
		}
		if c <= ' ' {
			break // Break on end of segment.
		}
		si++
		if isSep(c) {
			p = trimSeparatorPrefix(p[si:])
			si = 0
			break // Break on a separator.
		}
		if c == '.' || i >= ni {
			if ni == 11 || c != '.' {
				return "", frInvalidName
			}
			i = 8
			ni = 11
			continue
		}
		if strings.IndexByte(forbiddenChars, c) >= 0 {
			return "", frInvalidName
		}
		if c >= 0x80 {
			c = fsys.exCvt[c&0x7f] // To upper extended characters (SBCS).
		}
		if fsys.dbc_1st(c) {
			// Check second byte of DBC.
			var d byte
			if si < len(p) {
				d = p[si]
			}
			si++
			if !fsys.dbc_2nd(d) || i >= ni-1 {
				return "", frInvalidName
			}
			dp.fn[i] = c
			i++
			dp.fn[i] = d
			i++
		} else {
			if isLower(c) {
				c -= 0x20 // To upper.
			}
			dp.fn[i] = c
			i++
		}
	}
	if si > 0 {
		p = p[si:]
	}
	if i == 0 {
		return p, frInvalidName // Reject null name.
	}
	if dp.fn[0] == mskDDEM {
		// If the first character collides with DDEM, replace it with RDDEM.
		dp.fn[0] = mskRDDEM
	}
	cf := byte(0)
	if len(p) == 0 || p[0] <= ' ' {
		cf = nsLAST // Stopped at last segment (end of path).
	}
	dp.fn[nsFLAG] = cf
	return p, frOK
}

// get_fileinfo reads the SFN of the entry at dp into fno.
func (dp *dir) get_fileinfo(fno *FileInfo) {
	fno.fname[0] = 0 // Invalidate.
	if dp.sect == 0 {
		return // End of directory reached.
	} else if dp.obj.fs.fstype == fstypeExFAT {
		return
	}
	var si, di int
	lcf := byte(nsBODY)
	for si < 11 {
		// Get SFN.
		wc := dp.dir[si]
		si++
		if wc == ' ' {
			continue
		} else if wc == mskRDDEM {
			wc = mskDDEM
		}
		if si == 9 {
			fno.fname[di] = '.'
			di++
			lcf = nsEXT
		}
		if isUpper(wc) && dp.dir[dirNTresOff]&lcf != 0 {
			wc += 0x20 // To lower.
		}
		fno.fname[di] = wc
		di++
	}
	fno.fname[di] = 0 // Terminate the name.
	fno.altname[0] = 0
	fno.fattrib = dp.dir[dirAttrOff] & amMASK
	fno.fsize = int64(binary.LittleEndian.Uint32(dp.dir[dirFileSizeOff:]))
	fno.datetime.time = binary.LittleEndian.Uint16(dp.dir[dirModTimeOff:])
	fno.datetime.date = binary.LittleEndian.Uint16(dp.dir[dirModTimeOff+2:])
}
