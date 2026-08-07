module github.com/Wave-RF/WaveHouse/clients/go

// Library floor, deliberately lower than the server's pinned toolchain:
// the newest things this module uses are range-over-int and math/rand/v2
// (Go 1.22). Keep it a supported-releases floor, not a patch pin.
go 1.24
