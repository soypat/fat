package fat

import (
	"io"
	"testing"
)

// Benchmarks double as heap-allocation guards: every benchmark calls
// b.ReportAllocs() and the hot paths must report 0 allocs/op. Run with
//
//	go test -bench . -benchmem

// mountBench mounts the default FAT32 test image with logging disabled so
// that benchmark results measure filesystem work only.
func mountBench(b *testing.B) *FS {
	b.Helper()
	dev := DefaultFATByteBlocks(32000)
	var fs FS
	if err := fs.Mount(dev, dev.BlockSize(), ModeRW); err != nil {
		b.Fatal(err)
	}
	return &fs
}

// mountBenchExFAT formats and mounts an in-memory exFAT volume, skipping
// the benchmark on fat_noexfat/fat_nolfn builds.
func mountBenchExFAT(b *testing.B) *FS {
	b.Helper()
	if !exfatEnabled {
		b.Skip("exFAT support compiled out")
	}
	fsys, _, err := initTestExFAT(32000)
	if err != nil {
		b.Fatal(err)
	}
	return fsys
}

// benchFile creates a file of n bytes and returns its name.
func benchFile(b *testing.B, fsys *FS, name string, n int) {
	b.Helper()
	var f File
	if err := fsys.OpenFile(&f, name, ModeCreateAlways|ModeWrite); err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 512)
	for done := 0; done < n; done += len(buf) {
		if _, err := f.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkOpenClose(b *testing.B) {
	fsys := mountBench(b)
	var fp File
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fsys.OpenFile(&fp, "rootfile", ModeRead); err != nil {
			b.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSequential(b *testing.B) {
	fsys := mountBench(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeRead); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	buf := make([]byte, 1024)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := fp.Read(buf)
		if err == io.EOF {
			if _, err := fp.Seek(0, io.SeekStart); err != nil {
				b.Fatal(err)
			}
			continue
		}
		if err != nil || n == 0 {
			b.Fatal(n, err)
		}
	}
}

func BenchmarkSeekRead(b *testing.B) {
	fsys := mountBench(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeRead); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	size := fp.Size()
	buf := make([]byte, 64)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Stride through the file pseudo-randomly.
		off := int64((i+1)*4099) % (size - int64(len(buf)))
		if _, err := fp.Seek(off, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(&fp, buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirList(b *testing.B) {
	fsys := mountBench(b)
	var dp Dir
	if err := fsys.OpenDir(&dp, "rootdir"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := dp.ForEachFile(func(info *FileInfo) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteRewrite(b *testing.B) {
	fsys := mountBench(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	buf := make([]byte, 4096)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Steady-state overwrite: open existing, rewrite first 4KB, sync.
		if err := fsys.OpenFile(&fp, "bench.bin", ModeRW); err != nil {
			b.Fatal(err)
		}
		if _, err := fp.Write(buf); err != nil {
			b.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCreateWriteSmall(b *testing.B) {
	fsys := mountBench(b)
	var fp File
	buf := make([]byte, 64)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fsys.OpenFile(&fp, "small.bin", ModeCreateAlways|ModeWrite); err != nil {
			b.Fatal(err)
		}
		if _, err := fp.Write(buf); err != nil {
			b.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCreateRemove(b *testing.B) {
	fsys := mountBench(b)
	var fp File
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fsys.OpenFile(&fp, "gone.bin", ModeCreateNew|ModeWrite); err != nil {
			b.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			b.Fatal(err)
		}
		if err := fsys.Remove("gone.bin"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadAt(b *testing.B) {
	fsys := mountBench(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeRead); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	size := fp.Size()
	buf := make([]byte, 64)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Stride through the file pseudo-randomly.
		off := int64((i+1)*4099) % (size - int64(len(buf)))
		if _, err := fp.ReadAt(buf, off); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteAt(b *testing.B) {
	fsys := mountBench(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeRW); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	size := fp.Size()
	buf := make([]byte, 64)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Overwrite within the existing file so size stays constant.
		off := int64((i+1)*4099) % (size - int64(len(buf)))
		if _, err := fp.WriteAt(buf, off); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteString(b *testing.B) {
	fsys := mountBench(b)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeCreateAlways|ModeRW); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	const s = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	b.SetBytes(int64(len(s)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fp.WriteString(s); err != nil {
			b.Fatal(err)
		}
		if fp.Size() >= 8192 {
			// Rewind to keep the file from growing unboundedly.
			if _, err := fp.Seek(0, io.SeekStart); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTruncate(b *testing.B) {
	fsys := mountBench(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeRW); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Shrink one cluster then grow back: exercises remove_chain and
		// the chain-stretching seek every iteration.
		if err := fp.Truncate(4096); err != nil {
			b.Fatal(err)
		}
		if err := fp.Truncate(8192); err != nil {
			b.Fatal(err)
		}
	}
}

// exFAT variants of the benchmarks above: entry-set loading, name-hash
// lookup, NoFatChain cluster generation, bitmap allocation and entry-set
// stores must stay alloc-free too.

func BenchmarkExFATOpenClose(b *testing.B) {
	fsys := mountBenchExFAT(b)
	benchFile(b, fsys, "rootfile", 512)
	var fp File
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fsys.OpenFile(&fp, "rootfile", ModeRead); err != nil {
			b.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExFATReadSequential(b *testing.B) {
	fsys := mountBenchExFAT(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeRead); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	buf := make([]byte, 1024)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := fp.Read(buf)
		if err == io.EOF {
			if _, err := fp.Seek(0, io.SeekStart); err != nil {
				b.Fatal(err)
			}
			continue
		}
		if err != nil || n == 0 {
			b.Fatal(n, err)
		}
	}
}

func BenchmarkExFATWriteRewrite(b *testing.B) {
	fsys := mountBenchExFAT(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	buf := make([]byte, 4096)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fsys.OpenFile(&fp, "bench.bin", ModeRW); err != nil {
			b.Fatal(err)
		}
		if _, err := fp.Write(buf); err != nil {
			b.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExFATCreateRemove(b *testing.B) {
	fsys := mountBenchExFAT(b)
	var fp File
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fsys.OpenFile(&fp, "tmp.bin", ModeWrite|ModeCreateAlways); err != nil {
			b.Fatal(err)
		}
		if _, err := fp.WriteString("x"); err != nil {
			b.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			b.Fatal(err)
		}
		if err := fsys.Remove("tmp.bin"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExFATDirList(b *testing.B) {
	fsys := mountBenchExFAT(b)
	for i := 0; i < 8; i++ {
		benchFile(b, fsys, "rootdir/file"+string(rune('0'+i)), 512)
	}
	var dp Dir
	if err := fsys.OpenDir(&dp, "rootdir"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := dp.ForEachFile(func(info *FileInfo) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExFATTruncate(b *testing.B) {
	fsys := mountBenchExFAT(b)
	benchFile(b, fsys, "bench.bin", 8192)
	var fp File
	if err := fsys.OpenFile(&fp, "bench.bin", ModeRW); err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Shrink one cluster then grow back: exercises remove_chain with
		// bitmap frees and the chain-stretching seek every iteration.
		if err := fp.Truncate(4096); err != nil {
			b.Fatal(err)
		}
		if err := fp.Truncate(8192); err != nil {
			b.Fatal(err)
		}
	}
}

// TestBenchmarksDoNotAllocate pins the 0 allocs/op guarantee in the
// regular test suite so a regression fails `go test`, not just an eyeballed
// benchmark run.
func TestBenchmarksDoNotAllocate(t *testing.T) {
	if testing.Short() {
		t.Skip("full benchmark run, skipped in -short")
	}
	benches := []struct {
		name string
		fn   func(b *testing.B)
	}{
		{"OpenClose", BenchmarkOpenClose},
		{"ReadSequential", BenchmarkReadSequential},
		{"SeekRead", BenchmarkSeekRead},
		{"DirList", BenchmarkDirList},
		{"WriteRewrite", BenchmarkWriteRewrite},
		{"CreateWriteSmall", BenchmarkCreateWriteSmall},
		{"CreateRemove", BenchmarkCreateRemove},
		{"ReadAt", BenchmarkReadAt},
		{"WriteAt", BenchmarkWriteAt},
		{"WriteString", BenchmarkWriteString},
		{"Truncate", BenchmarkTruncate},
		{"ExFATOpenClose", BenchmarkExFATOpenClose},
		{"ExFATReadSequential", BenchmarkExFATReadSequential},
		{"ExFATWriteRewrite", BenchmarkExFATWriteRewrite},
		{"ExFATCreateRemove", BenchmarkExFATCreateRemove},
		{"ExFATDirList", BenchmarkExFATDirList},
		{"ExFATTruncate", BenchmarkExFATTruncate},
	}
	for _, bench := range benches {
		res := testing.Benchmark(bench.fn)
		if allocs := res.AllocsPerOp(); allocs != 0 {
			t.Errorf("%s: %d allocs/op (%d bytes/op), want 0",
				bench.name, allocs, res.AllocedBytesPerOp())
		}
	}
}
