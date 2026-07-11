//go:build !fat_nolfn

package fat

import (
	_ "embed"
	"encoding/binary"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

// lfnbuffer is the long file name working buffer. Compile with the fat_nolfn
// build tag to disable long file name support (FatFs' FF_USE_LFN=0) and
// reduce binary size.
type lfnbuffer = [lfnBufSize + 1]uint16

// fnamebuffer holds a file's name in [FileInfo], sized to fit a LFN.
type fnamebuffer = [lfnBufSize + 1]byte

// initNames prepares the filename handling state during mount.
func (fsys *FS) initNames() {
	_ = str16(fsys.lfnbuf[:0]) // include str16 utility into build for debugging.
	if fsys.codepage == nil {
		// Default to OEM code page 437 (U.S.) matching a FatFs build with
		// FF_CODE_PAGE=437. CP437 is single-byte: the DBCS range table is set
		// so that dbc_1st/dbc_2nd never match.
		fsys.codepage = ff_codepage(437)
		fsys.exCvt = _tblCT437[:]
		fsys.dbcTbl = [10]byte{0xFF}
	}
}

// pick_lfn picks a part of a filename from LFN entry.
func (fsys *FS) pick_lfn(dir []byte) bool {
	fsys.trace("pick_lfn")
	if binary.LittleEndian.Uint16(dir[ldirFstClusLO_Off:]) != 0 {
		return false
	}
	i := 13 * int((dir[ldirOrdOff]&^mskLLEF)-1) // Offset in LFN buffer.
	var wc uint16
	var s int
	for wc = 1; s < 13; s++ {
		uc := binary.LittleEndian.Uint16(dir[lfnOffsets[s]:])
		if wc != 0 {
			if i >= lfnBufSize+1 {
				return false
			}
			fsys.lfnbuf[i] = uc
			wc = uc
			i++
		} else if uc != maxu16 {
			return false
		}
	}
	if dir[ldirOrdOff]&mskLLEF != 0 && wc != 0 {
		// Put terminator if last LFN part and not terminated.
		if i >= lfnBufSize+1 {
			return false
		}
		fsys.lfnbuf[i] = 0
	}
	return true
}

// lfnlen returns the LFN length.
func (fsys *FS) lfnlen() (ln int) {
	for ; ln < len(fsys.lfnbuf) && fsys.lfnbuf[ln] != 0; ln++ {
	}
	return ln
}

var lfnOffsets = [...]byte{1, 3, 5, 7, 9, 14, 16, 18, 20, 22, 24, 28, 30}

func (fsys *FS) put_lfn(dir []byte, ord, sum byte) {
	fsys.trace("put_lfn", slog.Uint64("ord", uint64(ord)))
	// TODO(soypat): maybe this should receive a *dir and avoid two word copies?
	lfn := &fsys.lfnbuf
	dir[ldirChksumOff] = sum
	dir[ldirAttrOff] = amLFN
	dir[ldirTypeOff] = 0
	binary.LittleEndian.PutUint16(dir[ldirFstClusLO_Off:], 0)
	i := uint32(ord-1) * 13
	var wc uint16
	var s uint32
	for s < 13 {
		if wc != maxu16 {
			wc = lfn[i]
			i++
		}
		off := lfnOffsets[s]
		binary.LittleEndian.PutUint16(dir[off:], wc)
		if wc == 0 {
			wc = maxu16
		}
		s++
	}
	if wc == maxu16 || lfn[i] == 0 {
		ord |= mskLLEF
	}
	dir[ldirOrdOff] = ord
}

func (dp *dir) create_name(path string) (string, fileResult) {
	var (
		p    = path
		fsys = dp.obj.fs
		lfn  = fsys.lfnbuf[:]
		di   = 0
	)
	fsys.trace("dir:create_name")
	var wc uint16
	for {
		if len(p) == 0 {
			wc = 0
			break // Break on end of path.
		}
		uc, plen := utf8.DecodeRuneInString(p)
		if uc == utf8.RuneError {
			return "", frInvalidName
		}
		if uc >= 0x10000 {
			// Store high surrogate of the pair; low surrogate is stored below.
			r1, r2 := utf16.EncodeRune(uc)
			if di >= lfnBufSize {
				return "", frInvalidName
			}
			lfn[di] = uint16(r1)
			di++
			uc = r2
		}
		p = p[plen:]
		wc = uint16(uc)
		if isTermLFN(wc) || isSep(wc) {
			break // Break on end of path or a separator.
		}
		if strings.IndexByte(forbiddenChars, byte(wc)) >= 0 {
			return "", frInvalidName
		}
		if di >= lfnBufSize {
			return "", frInvalidName
		}
		lfn[di] = wc
		di++
	}
	var cf byte
	if isTermLFN(wc) {
		cf = nsLAST // Stopped at last segment (end of path).
	} else {
		p = trimSeparatorPrefix(p)
		if len(p) == 0 || isTermLFN(p[0]) {
			cf = nsLAST
		}
	}
	path = p // Returns next segment.

	for di > 0 {
		wc = lfn[di-1]
		if wc != ' ' && wc != '.' {
			break
		}
		di--
	}
	lfn[di] = 0
	if di == 0 {
		return path, frInvalidName // Reject null name.
	}
	var si int
	for si = 0; si < di && lfn[si] == ' '; si++ {
	}
	if si > 0 || lfn[si] == '.' {
		cf |= nsLOSS | nsLFN // Leading dot.
	}
	for di > 0 && lfn[di-1] != '.' {
		di-- // Find last dot (di<=si: no extension).
	}
	for i := 0; i < 11; i++ {
		dp.fn[i] = ' ' // memset(dp.fn, ' ', 11);
	}

	i := 0
	b := byte(0)
	ni := 8
	codepageEnabled := len(fsys.codepage) != 0
	for si < len(lfn) {
		wc = lfn[si]
		si++
		if wc == 0 {
			break
		}
		if wc == ' ' || (wc == '.' && si != di) {
			cf |= nsLOSS | nsLFN // Remove embedded spaces and dots.
			continue
		}
		if i >= ni || si == di {
			if ni == 11 {
				cf |= nsLOSS | nsLFN // Possible name extension overflow.
				break
			}
			if si != di {
				cf |= nsLOSS | nsLFN // Possible name body overflow.
			}
			if si > di {
				break
			}
			si = di
			i = 8
			ni = 11
			b <<= 2
			continue
		}

		if wc >= 0x80 {
			// Extended character.
			cf |= nsLFN // Flag LFN entry needs creation.
			if codepageEnabled {
				// SBCS configuration: Unicode -> ANSI/OEM code.
				wc = ff_uni2oem(rune(wc), fsys.codepage)
				if wc&0x80 != 0 {
					wc = uint16(fsys.exCvt[wc&0x7f]) // Convert extended character to upper (SBCS).
				}
			}
		}
		if wc >= 0x100 {
			// This is a DBC.
			if i >= ni-1 {
				// Possible field overflow.
				cf |= nsLOSS | nsLFN
				i = ni
				continue
			}
			dp.fn[i] = byte(wc >> 8)
			i++
		} else {
			if wc == 0 || strings.IndexByte("+,;=[]", byte(wc)) >= 0 {
				wc = '_'             // Replace illegal characters for SFN.
				cf |= nsLOSS | nsLFN // Flag the lossy conversion.
			} else {
				b |= b2u8(isUpper(wc)) << 1
				if isLower(wc) {
					b |= 1
					wc -= 0x20
				}
			}
		}
		dp.fn[i] = byte(wc)
		i++
	}
	if dp.fn[0] == mskDDEM {
		// If the first character collides with DDEM, replace it with RDDEM.
		dp.fn[0] = mskRDDEM
	}
	if ni == 8 {
		// Shift capital flags if no extension.
		b <<= 2
	}
	if b&0x0c == 0x0c || b&0x03 == 0x03 {
		//  LFN entry needs to be created if composite capitals.
		cf |= nsLFN
	}
	if cf&nsLFN == 0 {
		if b&1 != 0 {
			cf |= nsEXT
		}
		if b&4 != 0 {
			cf |= nsBODY
		}
	}
	dp.fn[nsFLAG] = cf // SFN is created into dp->fn[]
	return path, frOK
}

func (dp *dir) get_fileinfo(fno *FileInfo) {
	fsys := dp.obj.fs

	fno.fname[0] = 0 // Invalidate.
	if dp.sect == 0 {
		return // End of directory reached.
	} else if fsys.isExfat() {
		dp.get_fileinfo_exfat(fno)
		return
	}
	var si, di int
	var wc uint16
	if dp.blk_ofs != badLBA {
		// Get LFN if available.
		var hs uint16
		for fsys.lfnbuf[si] != 0 {
			wc = fsys.lfnbuf[si]
			si++
			if hs == 0 && isSurrogate(wc) {
				hs = wc // Low surrogate.
				continue
			}
			nw := put_utf8(rune(hs)<<16|rune(wc), fno.fname[di:])
			if nw == 0 {
				// Buffer overflow or wrong char.
				di = 0
				break
			}
			di += nw
			hs = 0
		}
		if hs != 0 {
			di = 0 // Broken surrogate pair.
		}
		fno.fname[di] = 0 // Terminate the LFN.
	}

	si, di = 0, 0
	for si < 11 {
		// Get SFN.
		wc = uint16(dp.dir[si])
		si++
		if wc == ' ' {
			continue
		} else if wc == mskRDDEM {
			wc = mskDDEM
		}
		if si == 9 && di < sfnBufSize {
			fno.altname[di] = '.'
			di++
		}
		b1 := fsys.dbc_1st(byte(wc))
		b2 := fsys.dbc_2nd(dp.dir[si])
		if b1 && si != 8 && si != 11 && b2 {
			wc = wc<<8 | uint16(dp.dir[si])
			si++
		}
		wc = ff_oem2uni(wc, fsys.codepage)

		if wc == 0 {
			di = 0 // Wrong char.
			break
		}

		nw := put_utf8(rune(wc), fno.altname[di:sfnBufSize])
		if nw == 0 {
			di = 0
			break
		}
		di += nw
	}
	// terminate altname
	fno.altname[di] = 0

	if fno.fname[0] == 0 {
		// LFN is invalid: altname needs to be copied to fname.
		if di == 0 {
			fno.fname[di] = '?'
			di++
		} else {
			si, di = 0, 0
			lcf := byte(nsBODY)
			for fno.altname[si] != 0 {
				wc = uint16(fno.altname[si])
				if wc == '.' {
					lcf = nsEXT
				}
				if isUpper(wc) && dp.dir[dirNTresOff]&lcf != 0 {
					wc += 0x20
				}
				fno.fname[di] = byte(wc)
				si++
				di++
			}
		}
		fno.fname[di] = 0 // Terminate the LFN.
		if dp.dir[dirNTresOff] == 0 {
			// Altname not needed nor case info exists.
			fno.altname[0] = 0
		}
	}
	fno.fattrib = dp.dir[dirAttrOff] & amMASK
	fno.fsize = int64(binary.LittleEndian.Uint32(dp.dir[dirFileSizeOff:]))
	fno.datetime.time = binary.LittleEndian.Uint16(dp.dir[dirModTimeOff:])
	fno.datetime.date = binary.LittleEndian.Uint16(dp.dir[dirModTimeOff+2:])
}

func put_utf8(r rune, buf []byte) int {
	if utf8.RuneLen(r) > len(buf) {
		return 0
	}
	return int(utf8.EncodeRune(buf, r))
}

// cmp_lfn returns true if entry matches LFN.
func (fsys *FS) cmp_lfn(dir []byte) bool {
	fsys.trace("fs:cmp_lfn")
	lfn := fsys.lfnbuf[:]
	if binary.LittleEndian.Uint16(dir[ldirFstClusLO_Off:]) != 0 {
		return false
	}
	i := int(dir[ldirOrdOff]&0x3F-1) * 13 // Offset in the LFN buffer.

	var wc uint16 = 1
	for s := 0; s < 13; s++ {
		uc := binary.LittleEndian.Uint16(dir[lfnOffsets[s]:])
		if wc != 0 {
			// TODO: optimize branching below after validated.
			lfnc := rune(lfn[i])
			w1 := ff_wtoupper(rune(uc))
			w2 := ff_wtoupper(lfnc)
			if i >= lfnBufSize+1 || w1 != w2 {
				return false
			}
			i++
			wc = uc
		} else {
			if uc != 0xFFFF {
				return false
			}
		}
	}
	return !(dir[ldirOrdOff]&mskLLEF != 0 && wc != 0 && lfn[i] != 0)
	// TODO(soypat): check if below is equivalent:
	// return dir[ldirOrdOff]&mskLLEF == 0 || wc == 0 || lfn[i] == 0
}

func (fsys *FS) gen_numname(dst, src []byte, lfn []uint16, seq uint32) {
	fsys.trace("fs:gen_numname")
	copy(dst[:11], src) // Prepare SFN to be modified.
	if seq > 5 {
		// On many collisions, generate a hash number instead of sequential number.
		sreg := seq
		for k := 0; lfn[k] != 0; k++ {
			// Create CRC as hash value.
			wc := lfn[k]
			for i := 0; i < 16; i++ {
				sreg = (sreg << 1) + uint32(wc&1)
				wc >>= 1
				if sreg&0x10000 != 0 {
					sreg ^= 0x11021
				}
			}
		}
		seq = sreg
	}

	// Make suffix with hexadecimal.
	var ns [8]byte
	i := 7
	for {
		c := byte((seq % 16) + '0')
		seq /= 16
		if c > '9' {
			c += 7
		}
		ns[i] = c
		i--
		if i == 0 || seq == 0 {
			break
		}
	}
	ns[i] = '~'

	// Append suffix to SFN body.
	j := 0
	for ; j < i && dst[j] != ' '; j++ {
		if fsys.dbc_1st(dst[j]) {
			if j == i-1 {
				break
			}
			j++
		}
	}

	// Append suffix.
	for {
		if i < 8 {
			dst[j] = ns[i]
			i++
		} else {
			dst[j] = ' '
		}
		j++
		if j >= 8 {
			break
		}
	}
}

func str16(s []uint16) string {
	if len(s) == 0 {
		return ""
	}
	var buf []byte
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == 0 || b >= utf8.RuneError {
			return string(buf)
		} else if b >= 0x80 {
			buf = append(buf, byte(b>>8))
		}
		buf = append(buf, byte(b))
	}
	return string(buf)
}

// offsets and lengths into the DBCS unicode code conversion tables.
const (
	dbcs932Len = 14780 * 2
	dbcs936Len = 43586 * 2
	dbcs949Len = 34098 * 2
	dbcs950Len = 27008 * 2

	dbcs932Off = 0
	dbcs936Off = dbcs932Off + dbcs932Len
	dbcs949Off = dbcs936Off + dbcs936Len
	dbcs950Off = dbcs949Off + dbcs949Len

	// calculated (read as "expected") length of the dbcs table.
	__dbcsTblLen = dbcs950Off + dbcs950Len
)

// offsets into the SBCS unicode code conversion tables in cp_oem2uni_le.
// Each table is 256 bytes long.
const (
	uc437Off = 0
	uc720Off = 256
	uc737Off = 512
	uc771Off = 768
	uc775Off = 1024
	uc850Off = 1280
	uc852Off = 1536
	uc855Off = 1792
	uc857Off = 2048
	uc860Off = 2304
	uc861Off = 2560
	uc862Off = 2816
	uc863Off = 3072
	uc864Off = 3328
	uc865Off = 3584
	uc866Off = 3840
	uc869Off = 4096
)

// cp_map is a list of supported codepages which index directly into cp_oem2uni_le.
var cp_map = [...]uint16{437, 720, 737, 771, 775, 850, 852, 855,
	857, 860, 861, 862, 863, 864, 865, 866, 869}

// embedded unicode tables. Storage format is 16-bit little endian.
// Code pages 900+ are separate due to their large size.
var (
	//go:embed embed/cp900_uni2oem_le.tbl
	cp900_uni2oem_le []byte
	//go:embed embed/cp900_oem2uni_le.tbl
	cp900_oem2uni_le []byte
	//go:embed embed/cp_oem2uni_le.tbl
	cp_oem2uni_le []byte
)

// ff_uni2oem converts a unicode character to an ANSI/OEM character, zero on error.
func ff_uni2oem(uni rune, codepage []byte) uint16 {
	// TODO(soypat): Shouldn't uni be a uint16? It seems like we don't support 32bit unicode here...
	if uni < 0x80 {
		return uint16(uni)
	} else if uni >= 0x10000 || len(codepage) != 256 {
		return 0 // DBCS not supported yet.
	}
	uc := uint16(uni)
	for c := uint16(0); c < 0x80; c++ {
		if uc == binary.LittleEndian.Uint16(codepage[c*2:]) {
			return (c + 0x80) & 0xFF
		}
	}
	return 0
}

// ff_oem2uni converts an OEM character to a unicode character, zero on error.
func ff_oem2uni(oem uint16, codepage []byte) uint16 {
	if oem < 0x80 {
		return oem
	} else if oem >= 0x100 && len(codepage) != 256 {
		return 0 // DBCS not supported yet.
	}
	offset := (oem - 0x80) * 2
	return binary.LittleEndian.Uint16(codepage[offset:])
}

// ff_codepage returns the unicode to OEM codepage table for the given codepage, nil on not found.
func ff_codepage(code uint16) []byte {
	if code >= 900 {
		println("code pages 900+ unsupported")
	}
	for i := 0; i < len(cp_map); i++ {
		if cp_map[i] == code {
			off := i * 256
			return cp_oem2uni_le[off : off+256]
		}
	}
	return nil
}

func ff_wtoupper(c rune) rune {
	return unicode.ToUpper(c)
}
