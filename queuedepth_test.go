package ltx_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

// BenchmarkQueueDepth tests various worker counts to find optimal queue depth.
// This helps identify the sweet spot for concurrent I/O operations.
func BenchmarkQueueDepth(b *testing.B) {
	const pageSize = 32768
	const pageN = 3200 // 100MB / 32KB

	// Create a temp file with encoded data
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "test.ltx")

	// Generate file once
	f, err := os.Create(filename)
	if err != nil {
		b.Fatal(err)
	}

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		b.Fatal(err)
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:  ltx.Version,
		PageSize: pageSize,
		Commit:   pageN,
		MinTXID:  1,
		MaxTXID:  1,
	}); err != nil {
		b.Fatal(err)
	}

	// Calculate checksum while encoding pages
	var dbChecksum ltx.Checksum
	page := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		_, _ = rand.Read(page)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			b.Fatal(err)
		}
		dbChecksum ^= ltx.ChecksumPage(pgno, page)
	}

	enc.SetPostApplyChecksum(ltx.ChecksumFlag | dbChecksum)
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}
	f.Close()

	// Test a fine-grained range of worker counts
	workerCounts := []int{1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32, 40, 48, 64}

	for _, workers := range workerCounts {
		b.Run(fmt.Sprintf("Workers%02d", workers), func(b *testing.B) {
			b.SetBytes(int64(pageN * pageSize))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				sd, err := ltx.NewSeekableDecoder(filename, workers)
				if err != nil {
					b.Fatal(err)
				}

				_, err = sd.DecodeAllPages()
				if err != nil {
					b.Fatal(err)
				}

				sd.Close()
			}
		})
	}
}
