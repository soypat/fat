package utf16x

import (
	"encoding/binary"
	"errors"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	// 0xd800-0xdc00 encodes the high 10 bits of a pair.
	// 0xdc00-0xe000 encodes the low 10 bits of a pair.
	// the value is those 20 bits plus 0x10000.
	surr1 = 0xd800
	surr2 = 0xdc00
	surr3 = 0xe000

	surrSelf = 0x10000
)

// The conditions replacementChar==unicode.ReplacementChar and
// maxRune==unicode.MaxRune are verified in the tests.
// Defining them locally avoids this package depending on package unicode.

const (
	replacementChar = '\uFFFD'     // Unicode replacement character
	maxRune         = '\U0010FFFF' // Maximum valid Unicode code point.
)

var (
	errMultiple2    = errors.New("UTF16 bytes length must be multiple of 2")
	errShortDst     = errors.New("short destination buffer")
	errInvalidUTF8  = errors.New("invalid utf8 sequence")
	errInvalidUTF16 = errors.New("invalid utf16 sequence")
)

func ToUTF8(dstUTF8, srcUTF16 []byte, order16 binary.ByteOrder) (int, error) {
	if len(srcUTF16)%2 != 0 {
		return 0, errMultiple2
	}
	n := 0
	for len(srcUTF16) > 1 {
		r, size := DecodeRune(srcUTF16, order16)
		if r == utf8.RuneError {
			return n, errInvalidUTF16
		} else if utf8.RuneLen(r) > len(dstUTF8[n:]) {
			return n, errShortDst
		}
		srcUTF16 = srcUTF16[size:]
		n += utf8.EncodeRune(dstUTF8[n:], r)
	}
	return n, nil
}

func FromUTF8(dst16, src8 []byte, order16 binary.ByteOrder) (int, error) {
	n := 0
	for len(src8) > 0 {
		if len(dst16[n:]) < 2 {
			return n, errShortDst
		}
		r1, size := utf8.DecodeRune(src8)
		if r1 == utf8.RuneError {
			return n, errInvalidUTF8
		} else if len(dst16[n:]) < 4 && utf16.IsSurrogate(r1) {
			return n, errShortDst
		}
		n += EncodeRune(dst16[n:], r1, order16)
		src8 = src8[size:]
	}
	return n, nil
}

func EncodeRune(dst16 []byte, v rune, order16 binary.ByteOrder) (sizeBytes int) {
	switch {
	case 0 <= v && v < surr1, surr3 <= v && v < surrSelf:
		// normal rune
		_ = dst16[1] // Eliminate bounds check.
		order16.PutUint16(dst16, uint16(v))
		return 2

	case surrSelf <= v && v <= maxRune:
		// needs surrogate sequence
		_ = dst16[3] // Eliminate bounds check.
		r1, r2 := utf16.EncodeRune(v)
		order16.PutUint16(dst16, uint16(r1))
		order16.PutUint16(dst16[2:], uint16(r2))
		return 4

	default:
		_ = dst16[1] // Eliminate bounds check.
		order16.PutUint16(dst16, uint16(replacementChar))
		return 2
	}
}

func DecodeRune(srcUTF16 []byte, order16 binary.ByteOrder) (r rune, size int) {
	_ = srcUTF16[1] // Eliminate bounds check.
	slen := len(srcUTF16)
	if slen == 0 {
		return '\uFFFD', 1
	}
	r = rune(order16.Uint16(srcUTF16))
	switch {
	case r < surr1, surr3 <= r:
		// normal rune
		return r, 2
	case surr1 <= r && r < surr2:
		_ = srcUTF16[3] // Eliminate bounds check.
		r2 := rune(order16.Uint16(srcUTF16[2:]))
		if !(surr2 <= r2 && r2 < surr3) {
			// Invalid surrogate sequence.
			return replacementChar, 2
		}
		// valid surrogate sequence
		return utf16.DecodeRune(r, r2), 4
	default:
		// invalid surrogate sequence
		return replacementChar, 2
	}
}
