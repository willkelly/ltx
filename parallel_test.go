package ltx_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

// BenchmarkParallelDecodeFile benchmarks parallel file decoding with random access.
func BenchmarkParallelDecodeFile(b *testing.B) {
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

	// Benchmark different worker counts
	for _, workers := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("Workers%d", workers), func(b *testing.B) {
			b.SetBytes(int64(pageN * pageSize))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, err := ltx.ParallelDecodeFile(filename, workers)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSeekableDecoderAll benchmarks SeekableDecoder decoding all pages at once.
func BenchmarkSeekableDecoderAll(b *testing.B) {
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

	// Benchmark different worker counts
	for _, workers := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("Workers%d", workers), func(b *testing.B) {
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

// BenchmarkSeekableDecoderBatches benchmarks SeekableDecoder decoding in batches.
// This demonstrates lower peak memory usage by processing pages in chunks.
func BenchmarkSeekableDecoderBatches(b *testing.B) {
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

	// Benchmark different batch sizes with 8 workers
	for _, batchSize := range []int{50, 100, 200, 400} {
		b.Run(fmt.Sprintf("Batch%d/Workers8", batchSize), func(b *testing.B) {
			b.SetBytes(int64(pageN * pageSize))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				sd, err := ltx.NewSeekableDecoder(filename, 8)
				if err != nil {
					b.Fatal(err)
				}

				// Process in batches
				for batch := uint32(0); batch < pageN; batch += uint32(batchSize) {
					end := batch + uint32(batchSize)
					if end > pageN {
						end = pageN
					}

					pgnos := make([]uint32, 0, end-batch)
					for pgno := batch + 1; pgno <= end; pgno++ {
						pgnos = append(pgnos, pgno)
					}

					_, err := sd.DecodePages(pgnos)
					if err != nil {
						b.Fatal(err)
					}
					// Pages are processed and can be garbage collected
				}

				sd.Close()
			}
		})
	}
}

// BenchmarkSeekableDecoderSingle benchmarks SeekableDecoder decoding individual pages.
func BenchmarkSeekableDecoderSingle(b *testing.B) {
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

	b.Run("RandomAccess", func(b *testing.B) {
		sd, err := ltx.NewSeekableDecoder(filename, 1)
		if err != nil {
			b.Fatal(err)
		}
		defer sd.Close()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Random page access
			pgno := uint32(rand.Intn(int(pageN))) + 1
			_, err := sd.DecodePage(pgno)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
