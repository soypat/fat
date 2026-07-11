//go:build !fat_nolfn

package fat

import (
	_ "embed"
	"encoding/binary"
	"log/slog"
	"strings"
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

// wtoupperCvt1 is the compressed up-conversion table for U+0000..U+0FFF and
// wtoupperCvt2 for U+1000..U+FFFF, ported verbatim from FatFs ffunicode.c.
// Layout: block base, then command<<8|block size, then for command 0 a
// conversion table of block-size words. Command 1 folds case pairs, commands
// 2..8 apply fixed shifts.
var wtoupperCvt1 = [...]uint16{
	/* Basic Latin */
	0x0061, 0x031A,
	/* Latin-1 Supplement */
	0x00E0, 0x0317,
	0x00F8, 0x0307,
	0x00FF, 0x0001, 0x0178,
	/* Latin Extended-A */
	0x0100, 0x0130,
	0x0132, 0x0106,
	0x0139, 0x0110,
	0x014A, 0x012E,
	0x0179, 0x0106,
	/* Latin Extended-B */
	0x0180, 0x004D, 0x0243, 0x0181, 0x0182, 0x0182, 0x0184, 0x0184, 0x0186, 0x0187, 0x0187, 0x0189, 0x018A, 0x018B, 0x018B, 0x018D, 0x018E, 0x018F, 0x0190, 0x0191, 0x0191, 0x0193, 0x0194, 0x01F6, 0x0196, 0x0197, 0x0198, 0x0198, 0x023D, 0x019B, 0x019C, 0x019D, 0x0220, 0x019F, 0x01A0, 0x01A0, 0x01A2, 0x01A2, 0x01A4, 0x01A4, 0x01A6, 0x01A7, 0x01A7, 0x01A9, 0x01AA, 0x01AB, 0x01AC, 0x01AC, 0x01AE, 0x01AF, 0x01AF, 0x01B1, 0x01B2, 0x01B3, 0x01B3, 0x01B5, 0x01B5, 0x01B7, 0x01B8, 0x01B8, 0x01BA, 0x01BB, 0x01BC, 0x01BC, 0x01BE, 0x01F7, 0x01C0, 0x01C1, 0x01C2, 0x01C3, 0x01C4, 0x01C5, 0x01C4, 0x01C7, 0x01C8, 0x01C7, 0x01CA, 0x01CB, 0x01CA,
	0x01CD, 0x0110,
	0x01DD, 0x0001, 0x018E,
	0x01DE, 0x0112,
	0x01F3, 0x0003, 0x01F1, 0x01F4, 0x01F4,
	0x01F8, 0x0128,
	0x0222, 0x0112,
	0x023A, 0x0009, 0x2C65, 0x023B, 0x023B, 0x023D, 0x2C66, 0x023F, 0x0240, 0x0241, 0x0241,
	0x0246, 0x010A,
	/* IPA Extensions */
	0x0253, 0x0040, 0x0181, 0x0186, 0x0255, 0x0189, 0x018A, 0x0258, 0x018F, 0x025A, 0x0190, 0x025C, 0x025D, 0x025E, 0x025F, 0x0193, 0x0261, 0x0262, 0x0194, 0x0264, 0x0265, 0x0266, 0x0267, 0x0197, 0x0196, 0x026A, 0x2C62, 0x026C, 0x026D, 0x026E, 0x019C, 0x0270, 0x0271, 0x019D, 0x0273, 0x0274, 0x019F, 0x0276, 0x0277, 0x0278, 0x0279, 0x027A, 0x027B, 0x027C, 0x2C64, 0x027E, 0x027F, 0x01A6, 0x0281, 0x0282, 0x01A9, 0x0284, 0x0285, 0x0286, 0x0287, 0x01AE, 0x0244, 0x01B1, 0x01B2, 0x0245, 0x028D, 0x028E, 0x028F, 0x0290, 0x0291, 0x01B7,
	/* Greek, Coptic */
	0x037B, 0x0003, 0x03FD, 0x03FE, 0x03FF,
	0x03AC, 0x0004, 0x0386, 0x0388, 0x0389, 0x038A,
	0x03B1, 0x0311,
	0x03C2, 0x0002, 0x03A3, 0x03A3,
	0x03C4, 0x0308,
	0x03CC, 0x0003, 0x038C, 0x038E, 0x038F,
	0x03D8, 0x0118,
	0x03F2, 0x000A, 0x03F9, 0x03F3, 0x03F4, 0x03F5, 0x03F6, 0x03F7, 0x03F7, 0x03F9, 0x03FA, 0x03FA,
	/* Cyrillic */
	0x0430, 0x0320,
	0x0450, 0x0710,
	0x0460, 0x0122,
	0x048A, 0x0136,
	0x04C1, 0x010E,
	0x04CF, 0x0001, 0x04C0,
	0x04D0, 0x0144,
	/* Armenian */
	0x0561, 0x0426,

	0x0000, /* EOT */
}

var wtoupperCvt2 = [...]uint16{
	/* Phonetic Extensions */
	0x1D7D, 0x0001, 0x2C63,
	/* Latin Extended Additional */
	0x1E00, 0x0196,
	0x1EA0, 0x015A,
	/* Greek Extended */
	0x1F00, 0x0608,
	0x1F10, 0x0606,
	0x1F20, 0x0608,
	0x1F30, 0x0608,
	0x1F40, 0x0606,
	0x1F51, 0x0007, 0x1F59, 0x1F52, 0x1F5B, 0x1F54, 0x1F5D, 0x1F56, 0x1F5F,
	0x1F60, 0x0608,
	0x1F70, 0x000E, 0x1FBA, 0x1FBB, 0x1FC8, 0x1FC9, 0x1FCA, 0x1FCB, 0x1FDA, 0x1FDB, 0x1FF8, 0x1FF9, 0x1FEA, 0x1FEB, 0x1FFA, 0x1FFB,
	0x1F80, 0x0608,
	0x1F90, 0x0608,
	0x1FA0, 0x0608,
	0x1FB0, 0x0004, 0x1FB8, 0x1FB9, 0x1FB2, 0x1FBC,
	0x1FCC, 0x0001, 0x1FC3,
	0x1FD0, 0x0602,
	0x1FE0, 0x0602,
	0x1FE5, 0x0001, 0x1FEC,
	0x1FF3, 0x0001, 0x1FFC,
	/* Letterlike Symbols */
	0x214E, 0x0001, 0x2132,
	/* Number forms */
	0x2170, 0x0210,
	0x2184, 0x0001, 0x2183,
	/* Enclosed Alphanumerics */
	0x24D0, 0x051A,
	0x2C30, 0x042F,
	/* Latin Extended-C */
	0x2C60, 0x0102,
	0x2C67, 0x0106, 0x2C75, 0x0102,
	/* Coptic */
	0x2C80, 0x0164,
	/* Georgian Supplement */
	0x2D00, 0x0826,
	/* Full-width */
	0xFF41, 0x031A,

	0x0000, /* EOT */
}

// ff_wtoupper up-converts a Unicode code point using FatFs' compressed
// conversion tables (ffunicode.c). The exFAT on-disk name hashes and the
// mkfs up-case table are derived from this exact mapping, so it must not
// be replaced with unicode.ToUpper which tracks a different (current)
// Unicode version.
func ff_wtoupper(uni rune) rune {
	if uni >= 0x10000 || uni < 0 {
		return uni // Only the BMP is up-converted.
	}
	uc := uint16(uni)
	p := wtoupperCvt1[:]
	if uc >= 0x1000 {
		p = wtoupperCvt2[:]
	}
	for i := 0; ; {
		bc := p[i] // Get the block base.
		i++
		if bc == 0 || uc < bc {
			break // Not matched.
		}
		nc := p[i] // Get processing command and block size.
		i++
		cmd := nc >> 8
		nc &= 0xFF
		if int(uc) < int(bc)+int(nc) {
			// In the block.
			switch cmd {
			case 0:
				uc = p[i+int(uc-bc)] // Table conversion.
			case 1:
				uc -= (uc - bc) & 1 // Case pairs.
			case 2:
				uc -= 16 // Shift -16.
			case 3:
				uc -= 32 // Shift -32.
			case 4:
				uc -= 48 // Shift -48.
			case 5:
				uc -= 26 // Shift -26.
			case 6:
				uc += 8 // Shift +8.
			case 7:
				uc -= 80 // Shift -80.
			case 8:
				uc -= 0x1C60 // Shift -0x1C60.
			}
			break
		}
		if cmd == 0 {
			i += int(nc) // Skip the conversion table.
		}
	}
	return rune(uc)
}
