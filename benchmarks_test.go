package ltx_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/superfly/ltx"
)

const benchPageSize = 32768

// TestGenerateFileSpec tests that file spec generation works correctly
func TestGenerateFileSpec(t *testing.T) {
	spec := generateLargeFileSpec(t, 10, benchPageSize)

	var buf bytes.Buffer
	_, err := spec.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	t.Logf("Generated %d bytes", buf.Len())

	// Try to decode it
	dec := ltx.NewDecoder(&buf)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	t.Logf("Header: %+v", dec.Header())

	pageData := make([]byte, benchPageSize)
	var hdr ltx.PageHeader
	count := 0
	for {
		err := dec.DecodePage(&hdr, pageData)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("DecodePage failed at page %d: %v", count, err)
		}
		count++
	}

	t.Logf("Decoded %d pages", count)

	if err := dec.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	t.Logf("Trailer: %+v", dec.Trailer())
}

// BenchmarkDecodeLargeFile benchmarks decoding a large LTX file with many pages.
// This should show improvement with Zstd compression over LZ4.
func BenchmarkDecodeLargeFile(b *testing.B) {
	for _, numPages := range []int{100, 500, 1000, 5000} {
		b.Run(formatPages(numPages), func(b *testing.B) {
			benchmarkDecodeLargeFile(b, numPages)
		})
	}
}

func benchmarkDecodeLargeFile(b *testing.B, numPages int) {
	// Generate file spec with random data
	spec := generateLargeFileSpec(b, numPages, benchPageSize)

	// Write spec to buffer
	var buf bytes.Buffer
	writeFileSpec(b, &buf, spec)
	fileData := buf.Bytes()

	// Calculate total data size for throughput reporting
	totalBytes := int64(numPages * benchPageSize)
	b.SetBytes(totalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dec := ltx.NewDecoder(bytes.NewReader(fileData))

		if err := dec.DecodeHeader(); err != nil {
			b.Fatal(err)
		}

		pageData := make([]byte, benchPageSize)
		var hdr ltx.PageHeader
		for {
			err := dec.DecodePage(&hdr, pageData)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}

		if err := dec.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompactMultipleFiles benchmarks compacting multiple LTX files into one.
func BenchmarkCompactMultipleFiles(b *testing.B) {
	for _, numFiles := range []int{2, 5, 10} {
		b.Run(formatFiles(numFiles), func(b *testing.B) {
			benchmarkCompactMultipleFiles(b, numFiles, 100)
		})
	}
}

func benchmarkCompactMultipleFiles(b *testing.B, numFiles, pagesPerFile int) {
	// Generate multiple file specs
	specs := make([]*ltx.FileSpec, numFiles)
	for i := 0; i < numFiles; i++ {
		specs[i] = generateFileSpec(b, i+1, pagesPerFile, benchPageSize)
	}

	// Pre-serialize all input files
	inputData := make([][]byte, numFiles)
	for i, spec := range specs {
		var buf bytes.Buffer
		writeFileSpec(b, &buf, spec)
		inputData[i] = buf.Bytes()
	}

	totalBytes := int64(numFiles * pagesPerFile * benchPageSize)
	b.SetBytes(totalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create readers from pre-serialized data
		readers := make([]io.Reader, numFiles)
		for j := range inputData {
			readers[j] = bytes.NewReader(inputData[j])
		}

		var output bytes.Buffer
		c, err := ltx.NewCompactor(&output, readers)
		if err != nil {
			b.Fatal(err)
		}

		if err := c.Compact(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeDatabaseTo benchmarks decoding an LTX snapshot to SQLite database format.
func BenchmarkDecodeDatabaseTo(b *testing.B) {
	for _, numPages := range []int{100, 500, 1000} {
		b.Run(formatPages(numPages), func(b *testing.B) {
			benchmarkDecodeDatabaseTo(b, numPages)
		})
	}
}

func benchmarkDecodeDatabaseTo(b *testing.B, numPages int) {
	// Generate snapshot file spec
	spec := generateSnapshotSpec(b, numPages, benchPageSize)

	// Write spec to buffer
	var buf bytes.Buffer
	writeFileSpec(b, &buf, spec)
	fileData := buf.Bytes()

	totalBytes := int64(numPages * benchPageSize)
	b.SetBytes(totalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dec := ltx.NewDecoder(bytes.NewReader(fileData))

		if err := dec.DecodeDatabaseTo(io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecoder_ParallelPages benchmarks parallel page decoding.
// This is for a future optimization where pages could be decoded in parallel.
// Currently this just measures baseline single-threaded performance.
func BenchmarkDecoder_ParallelPages(b *testing.B) {
	for _, numPages := range []int{100, 500, 1000} {
		b.Run(formatPages(numPages), func(b *testing.B) {
			benchmarkDecoderParallelPages(b, numPages, 4)
		})
	}
}

func benchmarkDecoderParallelPages(b *testing.B, numPages, goroutines int) {
	// Generate file spec with random data
	spec := generateLargeFileSpec(b, numPages, benchPageSize)

	// Write spec to buffer
	var buf bytes.Buffer
	writeFileSpec(b, &buf, spec)
	fileData := buf.Bytes()

	totalBytes := int64(numPages * benchPageSize)
	b.SetBytes(totalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Currently this is just serial decoding since parallel decoding
		// is not yet implemented. This benchmark establishes a baseline.
		dec := ltx.NewDecoder(bytes.NewReader(fileData))

		if err := dec.DecodeHeader(); err != nil {
			b.Fatal(err)
		}

		// For now, just decode serially to establish baseline
		// TODO: When parallel decoding is implemented, use goroutines here
		pageData := make([]byte, benchPageSize)
		var hdr ltx.PageHeader
		for {
			err := dec.DecodePage(&hdr, pageData)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}

		if err := dec.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// Helper functions

func generateLargeFileSpec(tb testing.TB, numPages, pageSize int) *ltx.FileSpec {
	tb.Helper()
	rnd := rand.New(rand.NewSource(42))

	pages := make([]ltx.PageSpec, numPages)
	for i := 0; i < numPages; i++ {
		pageData := make([]byte, pageSize)
		rnd.Read(pageData)
		pages[i] = ltx.PageSpec{
			Header: ltx.PageHeader{Pgno: uint32(i + 1)},
			Data:   pageData,
		}
	}

	// This is a snapshot (MinTXID=1), so PreApplyChecksum must be 0
	return &ltx.FileSpec{
		Header: ltx.Header{
			Version:          ltx.Version,
			PageSize:         uint32(pageSize),
			Commit:           uint32(numPages),
			MinTXID:          1,
			MaxTXID:          1,
			Timestamp:        1000,
			PreApplyChecksum: 0, // Must be 0 for snapshots
		},
		Pages: pages,
		Trailer: ltx.Trailer{
			PostApplyChecksum: calculateChecksum(pages),
		},
	}
}

func generateSnapshotSpec(tb testing.TB, numPages, pageSize int) *ltx.FileSpec {
	tb.Helper()
	// Snapshots must start with MinTXID = 1 and have all pages
	return generateLargeFileSpec(tb, numPages, pageSize)
}

func generateFileSpec(tb testing.TB, txid, numPages, pageSize int) *ltx.FileSpec {
	tb.Helper()
	rnd := rand.New(rand.NewSource(int64(txid * 1000)))

	pages := make([]ltx.PageSpec, numPages)
	for i := 0; i < numPages; i++ {
		pageData := make([]byte, pageSize)
		rnd.Read(pageData)
		pages[i] = ltx.PageSpec{
			Header: ltx.PageHeader{Pgno: uint32(i + 1)},
			Data:   pageData,
		}
	}

	var preApplyChecksum ltx.Checksum
	if txid > 1 {
		preApplyChecksum = ltx.ChecksumFlag | 0x1234567890abcdef
	}

	return &ltx.FileSpec{
		Header: ltx.Header{
			Version:          ltx.Version,
			PageSize:         uint32(pageSize),
			Commit:           uint32(numPages),
			MinTXID:          ltx.TXID(txid),
			MaxTXID:          ltx.TXID(txid),
			Timestamp:        int64(1000 * txid),
			PreApplyChecksum: preApplyChecksum,
		},
		Pages: pages,
		Trailer: ltx.Trailer{
			PostApplyChecksum: calculateChecksum(pages),
		},
	}
}

func calculateChecksum(pages []ltx.PageSpec) ltx.Checksum {
	var chksum ltx.Checksum = ltx.ChecksumFlag
	for _, page := range pages {
		chksum ^= ltx.ChecksumPage(page.Header.Pgno, page.Data)
	}
	return chksum
}

func formatPages(n int) string {
	if n >= 1000 {
		return fmt.Sprintf("%dk_pages", n/1000)
	}
	return fmt.Sprintf("%d_pages", n)
}

func formatFiles(n int) string {
	return fmt.Sprintf("%d_files", n)
}
