package fat

import (
	_ "embed"
	"encoding/binary"
	"unicode"
)

const (
	badFilesystemType = "fat: bad filesystem type"
)

const (
	negative1_32        = 0xffff_ffff
	badLBA       lba    = negative1_32
	mask28bits   uint32 = 0x0FFF_FFFF

	offsetMBRTable = 446  // Offset of partition table in the MBR.
	sizePartition  = 16   // Size of a partition table entry.
	mskDDEM        = 0xE5 // Deleted directory entry mark set to DIR_Name[0]
	mskRDDEM       = 0x05 // Replacement of the character collides with DDEM
	mskLLEF        = 0x40 // Last long entry flag in LDIR_Ord

)

const (
	nsFLAG   = 11   // Index of the name status byte
	nsLOSS   = 0x01 // Out of 8.3 format
	nsLFN    = 0x02 // Force to create LFN entry
	nsLAST   = 0x04 // Last segment
	nsBODY   = 0x08 // Lower case flag (body)
	nsEXT    = 0x10 // Lower case flag (ext)
	nsDOT    = 0x20 // Dot entry
	nsNOLFN  = 0x40 // Do not find LFN
	nsNONAME = 0x80 // Not followed

	fsiLeadSig    = 0   // FAT32 FSI: Leading signature (DWORD)
	fsiStrucSig   = 484 // FAT32 FSI: Structure signature (DWORD)
	fsiFree_Count = 488 // FAT32 FSI: Number of free clusters (DWORD)
	fsiNxt_Free   = 492 // FAT32 FSI: Last allocated cluster (DWORD)

	bsJmpBoot     = 0   // x86 jump instruction (3-byte)
	bsOEMName     = 3   // OEM name (8-byte)
	bpbBytsPerSec = 11  // Sector size [byte] (WORD)
	bpbSecPerClus = 13  // Cluster size [sector] (BYTE)
	bpbRsvdSecCnt = 14  // Size of reserved area [sector] (WORD)
	bpbNumFATs    = 16  // Number of FATs (BYTE)
	bpbRootEntCnt = 17  // Size of root directory area for FAT [entry] (WORD)
	bpbTotSec16   = 19  // Volume size (16-bit) [sector] (WORD)
	bpbMedia      = 21  // Media descriptor byte (BYTE)
	bpbFATSz16    = 22  // FAT size (16-bit) [sector] (WORD)
	bpbSecPerTrk  = 24  // Number of sectors per track for int13h [sector] (WORD)
	bpbNumHeads   = 26  // Number of heads for int13h (WORD)
	bpbHiddSec    = 28  // Volume offset from top of the drive (DWORD)
	bpbTotSec32   = 32  // Volume size (32-bit) [sector] (DWORD)
	bsDrvNum      = 36  // Physical drive number for int13h (BYTE)
	bsNTres       = 37  // WindowsNT error flag (BYTE)
	bsBootSig     = 38  // Extended boot signature (BYTE)
	bsVolID       = 39  // Volume serial number (DWORD)
	bsVolLab      = 43  // Volume label string (8-byte)
	bsFilSysType  = 54  // Filesystem type string (8-byte)
	bsBootCode    = 62  // Boot code (448-byte)
	bs55AA        = 510 // Signature word (WORD)

	bpbFATSz32     = 36 // FAT32: FAT size [sector] (DWORD)
	bpbExtFlags32  = 40 // FAT32: Extended flags (WORD)
	bpbFSVer32     = 42 // FAT32: Filesystem version (WORD)
	bpbRootClus32  = 44 // FAT32: Root directory cluster (DWORD)
	bpbFSInfo32    = 48 // FAT32: Offset of FSINFO sector (WORD)
	bpbBkBootSec32 = 50 // FAT32: Offset of backup boot sector (WORD)
	bsDrvNum32     = 64 // FAT32: Physical drive number for int13h (BYTE)
	bsNTres32      = 65 // FAT32: Error flag (BYTE)
	bsBootSig32    = 66 // FAT32: Extended boot signature (BYTE)
	bsVolID32      = 67 // FAT32: Volume serial number (DWORD)
	bsVolLab32     = 71 // FAT32: Volume label string (8-byte)
	bsFilSysType32 = 82 // FAT32: Filesystem type string (8-byte)
	bsBootCode32   = 90 // FAT32: Boot code (420-byte)

	bpbZeroedEx     = 11  // exFAT: MBZ field (53-byte)
	bpbVolOfsEx     = 64  // exFAT: Volume offset from top of the drive [sector] (QWORD)
	bpbTotSecEx     = 72  // exFAT: Volume size [sector] (QWORD)
	bpbFatOfsEx     = 80  // exFAT: FAT offset from top of the volume [sector] (DWORD)
	bpbFatSzEx      = 84  // exFAT: FAT size [sector] (DWORD)
	bpbDataOfsEx    = 88  // exFAT: Data offset from top of the volume [sector] (DWORD)
	bpbNumClusEx    = 92  // exFAT: Number of clusters (DWORD)
	bpbRootClusEx   = 96  // exFAT: Root directory start cluster (DWORD)
	bpbVolIDEx      = 100 // exFAT: Volume serial number (DWORD)
	bpbFSVerEx      = 104 // exFAT: Filesystem version (WORD)
	bpbVolFlagEx    = 106 // exFAT: Volume flags (WORD)
	bpbBytsPerSecEx = 108 // exFAT: Log2 of sector size in unit of byte (BYTE)
	bpbSecPerClusEx = 109 // exFAT: Log2 of cluster size in unit of sector (BYTE)
	bpbNumFATsEx    = 110 // exFAT: Number of FATs (BYTE)
	bpbDrvNumEx     = 111 // exFAT: Physical drive number for int13h (BYTE)
	bpbPercInUseEx  = 112 // exFAT: Percent in use (BYTE)
	bpbRsvdEx       = 113 // exFAT: Reserved (7-byte)
	bsBootCodeEx    = 120 // exFAT: Boot code (390-byte)
)

const (
	sizeDirEntry  = 32         // Size of a directory entry
	maxDIR        = 0x200000   // Max size of FAT directory
	maxDIREx      = 0x10000000 // Max size of exFAT directory
	clustMaxFAT12 = 0xFF5      // Max FAT12 clusters (differs from specs, but right for real DOS/Windows behavior)
	clustMaxFAT16 = 0xFFF5     // Max FAT16 clusters (differs from specs, but right for real DOS/Windows behavior)
	clustMaxFAT32 = 0x0FFFFFF5 // Max FAT32 clusters (not specified, practical limit)
	clustMaxExFAT = 0x7FFFFFFD // Max exFAT clusters (differs from specs, implementation limit)
)

// CT*: SBCS up-case tables.
// DC*: DBCS up-case tables.
var (
	_tblDC932 = [10]byte{0x81, 0x9F, 0xE0, 0xFC, 0x40, 0x7E, 0x80, 0xFC, 0x00, 0x00}
	_tblDC936 = [10]byte{0x81, 0xFE, 0x00, 0x00, 0x40, 0x7E, 0x80, 0xFE, 0x00, 0x00}
	_tblDC949 = [10]byte{0x81, 0xFE, 0x00, 0x00, 0x41, 0x5A, 0x61, 0x7A, 0x81, 0xFE}
	_tblDC950 = [10]byte{0x81, 0xFE, 0x00, 0x00, 0x40, 0x7E, 0xA1, 0xFE, 0x00, 0x00}

	_tblCT437 = [128]byte{0x80, 0x9A, 0x45, 0x41, 0x8E, 0x41, 0x8F, 0x80, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x8E, 0x8F, 0x90, 0x92, 0x92, 0x4F, 0x99, 0x4F, 0x55, 0x55, 0x59, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0x41, 0x49, 0x4F, 0x55, 0xA5, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT720 = [128]byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT737 = [128]byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x92, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0xAA, 0x92, 0x93, 0x94, 0x95, 0x96, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0x97, 0xEA, 0xEB, 0xEC, 0xE4, 0xED, 0xEE, 0xEF, 0xF5, 0xF0, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT771 = [128]byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDC, 0xDE, 0xDE, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xF0, 0xF0, 0xF2, 0xF2, 0xF4, 0xF4, 0xF6, 0xF6, 0xF8, 0xF8, 0xFA, 0xFA, 0xFC, 0xFC, 0xFE, 0xFF}
	_tblCT775 = [128]byte{0x80, 0x9A, 0x91, 0xA0, 0x8E, 0x95, 0x8F, 0x80, 0xAD, 0xED, 0x8A, 0x8A, 0xA1, 0x8D, 0x8E, 0x8F, 0x90, 0x92, 0x92, 0xE2, 0x99, 0x95, 0x96, 0x97, 0x97, 0x99, 0x9A, 0x9D, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xE0, 0xA3, 0xA3, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xB5, 0xB6, 0xB7, 0xB8, 0xBD, 0xBE, 0xC6, 0xC7, 0xA5, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE5, 0xE5, 0xE6, 0xE3, 0xE8, 0xE8, 0xEA, 0xEA, 0xEE, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT850 = [128]byte{0x43, 0x55, 0x45, 0x41, 0x41, 0x41, 0x41, 0x43, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x41, 0x41, 0x45, 0x92, 0x92, 0x4F, 0x4F, 0x4F, 0x55, 0x55, 0x59, 0x4F, 0x55, 0x4F, 0x9C, 0x4F, 0x9E, 0x9F, 0x41, 0x49, 0x4F, 0x55, 0xA5, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0x41, 0x41, 0x41, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0x41, 0x41, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD1, 0xD1, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x49, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0x49, 0xDF, 0x4F, 0xE1, 0x4F, 0x4F, 0x4F, 0x4F, 0xE6, 0xE8, 0xE8, 0x55, 0x55, 0x55, 0x59, 0x59, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT852 = [128]byte{0x80, 0x9A, 0x90, 0xB6, 0x8E, 0xDE, 0x8F, 0x80, 0x9D, 0xD3, 0x8A, 0x8A, 0xD7, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x91, 0xE2, 0x99, 0x95, 0x95, 0x97, 0x97, 0x99, 0x9A, 0x9B, 0x9B, 0x9D, 0x9E, 0xAC, 0xB5, 0xD6, 0xE0, 0xE9, 0xA4, 0xA4, 0xA6, 0xA6, 0xA8, 0xA8, 0xAA, 0x8D, 0xAC, 0xB8, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBD, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC6, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD1, 0xD1, 0xD2, 0xD3, 0xD2, 0xD5, 0xD6, 0xD7, 0xB7, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE3, 0xD5, 0xE6, 0xE6, 0xE8, 0xE9, 0xE8, 0xEB, 0xED, 0xED, 0xDD, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xEB, 0xFC, 0xFC, 0xFE, 0xFF}
	_tblCT855 = [128]byte{0x81, 0x81, 0x83, 0x83, 0x85, 0x85, 0x87, 0x87, 0x89, 0x89, 0x8B, 0x8B, 0x8D, 0x8D, 0x8F, 0x8F, 0x91, 0x91, 0x93, 0x93, 0x95, 0x95, 0x97, 0x97, 0x99, 0x99, 0x9B, 0x9B, 0x9D, 0x9D, 0x9F, 0x9F, 0xA1, 0xA1, 0xA3, 0xA3, 0xA5, 0xA5, 0xA7, 0xA7, 0xA9, 0xA9, 0xAB, 0xAB, 0xAD, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB6, 0xB6, 0xB8, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBE, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC7, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD1, 0xD1, 0xD3, 0xD3, 0xD5, 0xD5, 0xD7, 0xD7, 0xDD, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xE0, 0xDF, 0xE0, 0xE2, 0xE2, 0xE4, 0xE4, 0xE6, 0xE6, 0xE8, 0xE8, 0xEA, 0xEA, 0xEC, 0xEC, 0xEE, 0xEE, 0xEF, 0xF0, 0xF2, 0xF2, 0xF4, 0xF4, 0xF6, 0xF6, 0xF8, 0xF8, 0xFA, 0xFA, 0xFC, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT857 = [128]byte{0x80, 0x9A, 0x90, 0xB6, 0x8E, 0xB7, 0x8F, 0x80, 0xD2, 0xD3, 0xD4, 0xD8, 0xD7, 0x49, 0x8E, 0x8F, 0x90, 0x92, 0x92, 0xE2, 0x99, 0xE3, 0xEA, 0xEB, 0x98, 0x99, 0x9A, 0x9D, 0x9C, 0x9D, 0x9E, 0x9E, 0xB5, 0xD6, 0xE0, 0xE9, 0xA5, 0xA5, 0xA6, 0xA6, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC7, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0x49, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE5, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xDE, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT860 = [128]byte{0x80, 0x9A, 0x90, 0x8F, 0x8E, 0x91, 0x86, 0x80, 0x89, 0x89, 0x92, 0x8B, 0x8C, 0x98, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x8C, 0x99, 0xA9, 0x96, 0x9D, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0x86, 0x8B, 0x9F, 0x96, 0xA5, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT861 = [128]byte{0x80, 0x9A, 0x90, 0x41, 0x8E, 0x41, 0x8F, 0x80, 0x45, 0x45, 0x45, 0x8B, 0x8B, 0x8D, 0x8E, 0x8F, 0x90, 0x92, 0x92, 0x4F, 0x99, 0x8D, 0x55, 0x97, 0x97, 0x99, 0x9A, 0x9D, 0x9C, 0x9D, 0x9E, 0x9F, 0xA4, 0xA5, 0xA6, 0xA7, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT862 = [128]byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0x41, 0x49, 0x4F, 0x55, 0xA5, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT863 = [128]byte{0x43, 0x55, 0x45, 0x41, 0x41, 0x41, 0x86, 0x43, 0x45, 0x45, 0x45, 0x49, 0x49, 0x8D, 0x41, 0x8F, 0x45, 0x45, 0x45, 0x4F, 0x45, 0x49, 0x55, 0x55, 0x98, 0x4F, 0x55, 0x9B, 0x9C, 0x55, 0x55, 0x9F, 0xA0, 0xA1, 0x4F, 0x55, 0xA4, 0xA5, 0xA6, 0xA7, 0x49, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT864 = [128]byte{0x80, 0x9A, 0x45, 0x41, 0x8E, 0x41, 0x8F, 0x80, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x8E, 0x8F, 0x90, 0x92, 0x92, 0x4F, 0x99, 0x4F, 0x55, 0x55, 0x59, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0x41, 0x49, 0x4F, 0x55, 0xA5, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT865 = [128]byte{0x80, 0x9A, 0x90, 0x41, 0x8E, 0x41, 0x8F, 0x80, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x8E, 0x8F, 0x90, 0x92, 0x92, 0x4F, 0x99, 0x4F, 0x55, 0x55, 0x59, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0x41, 0x49, 0x4F, 0x55, 0xA5, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT866 = [128]byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xF0, 0xF0, 0xF2, 0xF2, 0xF4, 0xF4, 0xF6, 0xF6, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF}
	_tblCT869 = [128]byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x86, 0x9C, 0x8D, 0x8F, 0x90, 0x91, 0x90, 0x92, 0x95, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xA4, 0xA5, 0xA6, 0xD9, 0xDA, 0xDB, 0xDC, 0xA7, 0xA8, 0xDF, 0xA9, 0xAA, 0xAC, 0xAD, 0xB5, 0xB6, 0xB7, 0xB8, 0xBD, 0xBE, 0xC6, 0xC7, 0xCF, 0xCF, 0xD0, 0xEF, 0xF0, 0xF1, 0xD1, 0xD2, 0xD3, 0xF5, 0xD4, 0xF7, 0xF8, 0xF9, 0xD5, 0x96, 0x95, 0x98, 0xFE, 0xFF}
)

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
