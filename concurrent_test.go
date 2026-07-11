package fat

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
)

// TestConcurrentMixedOps hammers one FS from several goroutines doing
// mixed file and metadata operations. Run with -race; correctness of the
// serialized results is checked per goroutine.
func TestConcurrentMixedOps(t *testing.T) {
	fsys, _ := initTestFATWithLogger(32000, nil)
	runConcurrentMixedOps(t, fsys)
}

// TestConcurrentMixedOpsExFAT runs the same scenario on an exFAT volume:
// the shared FS.dirbuf entry-set scratchpad must be protected by the
// single FS lock exactly like the FAT window.
func TestConcurrentMixedOpsExFAT(t *testing.T) {
	skipIfNoExFAT(t)
	fsys, _, err := initTestExFAT(32000)
	if err != nil {
		t.Fatal(err)
	}
	runConcurrentMixedOps(t, fsys)
}

func runConcurrentMixedOps(t *testing.T, fsys *FS) {
	shared := []byte("shared file contents for concurrent ReadAt")
	var sharedFp File
	if err := fsys.OpenFile(&sharedFp, "shared.bin", ModeWrite|ModeCreateAlways); err != nil {
		t.Fatal(err)
	}
	if _, err := sharedFp.Write(shared); err != nil {
		t.Fatal(err)
	}
	if err := sharedFp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fsys.OpenFile(&sharedFp, "shared.bin", ModeRead); err != nil {
		t.Fatal(err)
	}
	defer sharedFp.Close()

	const goroutines = 8
	const iters = 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			path := fmt.Sprintf("g%02d.bin", g)
			data := bytes.Repeat([]byte{byte(g + 1)}, 100)
			for i := 0; i < iters; i++ {
				var fp File
				if err := fsys.OpenFile(&fp, path, ModeRW|ModeCreateAlways); err != nil {
					errs <- fmt.Errorf("g%d OpenFile: %w", g, err)
					return
				}
				if _, err := fp.Write(data); err != nil {
					errs <- fmt.Errorf("g%d Write: %w", g, err)
					return
				}
				got := make([]byte, len(data))
				if _, err := fp.ReadAt(got, 0); err != nil {
					errs <- fmt.Errorf("g%d ReadAt own: %w", g, err)
					return
				}
				if !bytes.Equal(got, data) {
					errs <- fmt.Errorf("g%d own data mismatch", g)
					return
				}
				if err := fp.Close(); err != nil {
					errs <- fmt.Errorf("g%d Close: %w", g, err)
					return
				}

				// Parallel ReadAt on the one shared handle.
				got = make([]byte, len(shared))
				if _, err := sharedFp.ReadAt(got, 0); err != nil {
					errs <- fmt.Errorf("g%d ReadAt shared: %w", g, err)
					return
				}
				if !bytes.Equal(got, shared) {
					errs <- fmt.Errorf("g%d shared data mismatch", g)
					return
				}

				var info FileInfo
				if err := fsys.Stat(path, &info); err != nil {
					errs <- fmt.Errorf("g%d Stat: %w", g, err)
					return
				}
				if info.Size() != int64(len(data)) {
					errs <- fmt.Errorf("g%d Stat size = %d, want %d", g, info.Size(), len(data))
					return
				}

				var dp Dir
				if err := fsys.OpenDir(&dp, "/"); err != nil {
					errs <- fmt.Errorf("g%d OpenDir: %w", g, err)
					return
				}
				if err := dp.ForEachFile(func(*FileInfo) error { return nil }); err != nil {
					errs <- fmt.Errorf("g%d dir iteration: %w", g, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestConcurrentCloseRace races Close against ReadAt on the same File:
// every call must return either valid data or frInvalidObject, never
// panic or corrupt.
func TestConcurrentCloseRace(t *testing.T) {
	fsys, _ := initTestFATWithLogger(32000, nil)
	var fp File
	if err := fsys.OpenFile(&fp, "f.bin", ModeWrite|ModeCreateAlways); err != nil {
		t.Fatal(err)
	}
	if _, err := fp.Write(bytes.Repeat([]byte{0xa5}, 300)); err != nil {
		t.Fatal(err)
	}
	if err := fp.Close(); err != nil {
		t.Fatal(err)
	}

	for round := 0; round < 50; round++ {
		if err := fsys.OpenFile(&fp, "f.bin", ModeRead); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := make([]byte, 64)
				for {
					_, err := fp.ReadAt(buf, 0)
					if err != nil && err != io.EOF {
						if err != frInvalidObject {
							t.Errorf("ReadAt after close: %v", err)
						}
						return
					}
				}
			}()
		}
		if err := fp.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
	}
}
