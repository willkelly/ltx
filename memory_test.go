package ltx_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/superfly/ltx"
)

// TestSeekableDecoderMemoryUsage demonstrates the peak memory difference
// between loading all pages vs batched processing.
func TestSeekableDecoderMemoryUsage(t *testing.T) {
	const pageSize = 32768
	const pageN = 3200 // 100MB / 32KB

	// Create a temp file with encoded data
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test.ltx")

	// Generate file
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		t.Fatal(err)
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:  ltx.Version,
		PageSize: pageSize,
		Commit:   pageN,
		MinTXID:  1,
		MaxTXID:  1,
	}); err != nil {
		t.Fatal(err)
	}

	var dbChecksum ltx.Checksum
	page := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		_, _ = rand.Read(page)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			t.Fatal(err)
		}
		dbChecksum ^= ltx.ChecksumPage(pgno, page)
	}

	enc.SetPostApplyChecksum(ltx.ChecksumFlag | dbChecksum)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Test 1: Load all pages at once
	t.Run("LoadAll", func(t *testing.T) {
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		heapBefore := m1.Alloc

		sd, err := ltx.NewSeekableDecoder(filename, 8)
		if err != nil {
			t.Fatal(err)
		}
		defer sd.Close()

		allPages, err := sd.DecodeAllPages()
		if err != nil {
			t.Fatal(err)
		}

		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		heapPeak := m2.Alloc

		t.Logf("Loaded %d pages", len(allPages))
		t.Logf("Heap before: %d MB", heapBefore/1024/1024)
		t.Logf("Heap peak:   %d MB", heapPeak/1024/1024)
		t.Logf("Heap delta:  %d MB", (heapPeak-heapBefore)/1024/1024)

		// Keep reference to prevent GC
		_ = allPages
	})

	// Test 2: Process in batches - test different batch sizes
	for _, batchSize := range []int{50, 100, 200, 400, 800} {
		t.Run(testName(batchSize), func(t *testing.T) {
			runtime.GC()
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)
			heapBefore := m1.Alloc

			sd, err := ltx.NewSeekableDecoder(filename, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer sd.Close()

			maxHeap := uint64(0)
			processedPages := 0

			for batch := uint32(0); batch < pageN; batch += uint32(batchSize) {
				end := batch + uint32(batchSize)
				if end > pageN {
					end = pageN
				}

				pgnos := make([]uint32, 0, end-batch)
				for pgno := batch + 1; pgno <= end; pgno++ {
					pgnos = append(pgnos, pgno)
				}

				pages, err := sd.DecodePages(pgnos)
				if err != nil {
					t.Fatal(err)
				}

				processedPages += len(pages)

				// Measure heap after each batch
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.Alloc > maxHeap {
					maxHeap = m.Alloc
				}

				// Process pages (simulate work)
				for _, data := range pages {
					_ = data[0] // Touch the data
				}

				// pages map goes out of scope here, available for GC
			}

			runtime.GC() // Force GC to see final memory
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)
			heapAfter := m2.Alloc

			t.Logf("Processed %d pages in batches of %d", processedPages, batchSize)
			t.Logf("Heap before: %d MB", heapBefore/1024/1024)
			t.Logf("Heap peak:   %d MB", maxHeap/1024/1024)
			t.Logf("Heap after:  %d MB", heapAfter/1024/1024)
			t.Logf("Peak delta:  %d MB", (maxHeap-heapBefore)/1024/1024)
		})
	}
}

func testName(batchSize int) string {
	return "Batched" + fmt.Sprintf("%d", batchSize)
}

// TestSeekableDecoderDecodeInBatches tests the convenience batching API.
func TestSeekableDecoderDecodeInBatches(t *testing.T) {
	const pageSize = 32768
	const pageN = 3200 // 100MB / 32KB

	// Create a temp file with encoded data
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test.ltx")

	// Generate file
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		t.Fatal(err)
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:  ltx.Version,
		PageSize: pageSize,
		Commit:   pageN,
		MinTXID:  1,
		MaxTXID:  1,
	}); err != nil {
		t.Fatal(err)
	}

	var dbChecksum ltx.Checksum
	page := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		_, _ = rand.Read(page)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			t.Fatal(err)
		}
		dbChecksum ^= ltx.ChecksumPage(pgno, page)
	}

	enc.SetPostApplyChecksum(ltx.ChecksumFlag | dbChecksum)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Test the DecodeInBatches API
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	heapBefore := m1.Alloc

	sd, err := ltx.NewSeekableDecoder(filename, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer sd.Close()

	maxHeap := uint64(0)
	totalPages := 0
	batchCount := 0

	err = sd.DecodeInBatches(func(pages map[uint32][]byte) error {
		batchCount++
		totalPages += len(pages)

		// Measure heap after each batch
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.Alloc > maxHeap {
			maxHeap = m.Alloc
		}

		// Process pages (simulate work)
		for _, data := range pages {
			_ = data[0] // Touch the data
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	heapAfter := m2.Alloc

	t.Logf("Processed %d pages in %d batches (default size 800)", totalPages, batchCount)
	t.Logf("Heap before: %d MB", heapBefore/1024/1024)
	t.Logf("Heap peak:   %d MB", maxHeap/1024/1024)
	t.Logf("Heap after:  %d MB", heapAfter/1024/1024)
	t.Logf("Peak delta:  %d MB", (maxHeap-heapBefore)/1024/1024)

	if totalPages != int(pageN) {
		t.Errorf("Expected %d pages, got %d", pageN, totalPages)
	}

	expectedBatches := (int(pageN) + 799) / 800 // ceil division
	if batchCount != expectedBatches {
		t.Errorf("Expected %d batches, got %d", expectedBatches, batchCount)
	}
}
