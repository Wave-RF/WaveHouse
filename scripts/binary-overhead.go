//go:build ignore

// binary-overhead reports the total size of a Go binary and the number of
// bytes occupied by DWARF debug sections — i.e. how much smaller the binary
// would be if linked with `-s -w`. Used by scripts/size.sh to show the
// release-equivalent size next to the current debug-build size.
//
// Reads the binary directly via debug/macho or debug/elf so we don't depend
// on python, jq, or gsa's JSON schema. Build tag `ignore` keeps it out of
// the main module's compile graph; invoke via:
//
//	go run scripts/binary-overhead.go bin/wavehouse
//
// Output is two lines, key=BYTES, suitable for `eval` or shell read:
//
//	total_bytes=71094066
//	dwarf_bytes=15703361
package main

import (
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <binary>\n", os.Args[0])
		os.Exit(2)
	}
	path := os.Args[1]

	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat %s: %v\n", path, err)
		os.Exit(1)
	}
	total := info.Size()

	dwarf, err := dwarfBytes(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
		os.Exit(1)
	}

	fmt.Printf("total_bytes=%d\n", total)
	fmt.Printf("dwarf_bytes=%d\n", dwarf)
}

func dwarfBytes(path string) (int64, error) {
	if mf, err := macho.Open(path); err == nil {
		defer mf.Close()
		var sum int64
		for _, s := range mf.Sections {
			if isDebug(s.Name) || isDebug(s.Seg) {
				sum += int64(s.Size)
			}
		}
		return sum, nil
	}
	if ef, err := elf.Open(path); err == nil {
		defer ef.Close()
		var sum int64
		for _, s := range ef.Sections {
			if isDebug(s.Name) {
				sum += int64(s.Size)
			}
		}
		return sum, nil
	}
	if pf, err := pe.Open(path); err == nil {
		defer pf.Close()
		var sum int64
		for _, s := range pf.Sections {
			if isDebug(s.Name) {
				sum += int64(s.Size)
			}
		}
		return sum, nil
	}
	return 0, fmt.Errorf("unsupported binary format (not Mach-O, ELF, or PE)")
}

func isDebug(name string) bool {
	n := strings.ToLower(name)
	return strings.Contains(n, "dwarf") || strings.Contains(n, "debug")
}
