//go:build windows

package node

import (
	"syscall"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

var (
	kernel32               = syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleOutputCP = kernel32.NewProc("GetConsoleOutputCP")
	procGetACP             = kernel32.NewProc("GetACP")
)

// currentCodepage reports the code page a console child writes its output in.
// GetConsoleOutputCP is what redirected console programs use; if it is
// unavailable (no console) we fall back to the system ANSI code page.
func currentCodepage() int {
	if r, _, _ := procGetConsoleOutputCP.Call(); r != 0 {
		return int(r)
	}
	r, _, _ := procGetACP.Call()
	return int(r)
}

func encodingForCP(cp int) encoding.Encoding {
	switch cp {
	case 936: // GBK / GB2312 (Simplified Chinese); GB18030 is a superset
		return simplifiedchinese.GB18030
	case 950: // Big5 (Traditional Chinese)
		return traditionalchinese.Big5
	case 932: // Shift-JIS (Japanese)
		return japanese.ShiftJIS
	case 949: // EUC-KR (Korean)
		return korean.EUCKR
	case 1250:
		return charmap.Windows1250
	case 1251:
		return charmap.Windows1251
	case 1252:
		return charmap.Windows1252
	case 437:
		return charmap.CodePage437
	case 850:
		return charmap.CodePage850
	case 866:
		return charmap.CodePage866
	}
	// 65001 (UTF-8) or anything unmapped: leave the bytes as-is.
	return nil
}

// toUTF8 converts raw command output (in the Windows console code page) to
// UTF-8. UTF-8/unknown code pages and any transcode error fall back to the
// raw bytes, so output is never lost.
func toUTF8(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	enc := encodingForCP(currentCodepage())
	if enc == nil {
		return string(b)
	}
	out, err := enc.NewDecoder().Bytes(b)
	if err != nil {
		return string(b)
	}
	return string(out)
}
