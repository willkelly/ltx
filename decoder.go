package ltx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// checksumJob represents a page to checksum.
type checksumJob struct {
	pgno uint32
	data []byte
}

// Decoder represents a decoder of an LTX file.
type Decoder struct {
	r  io.Reader   // main reader
	zr *lz4.Reader // lz4 reader

	header    Header
	trailer   Trailer
	pageIndex map[uint32]PageIndexElem
	state     string

	chksum     Checksum
	hash       hash.Hash64 // file checksum hasher
	pageHasher hash.Hash64 // reusable hasher for per-page checksums (not used when parallel)
	pageN      int         // pages read
	n          int64       // bytes read

	// Parallel checksumming
	checksumWorkers        int
	checksumJobs           chan checksumJob
	checksumWg             sync.WaitGroup
	checksumWorkerResults  []Checksum     // per-worker XOR accumulators
	bufferPool             *sync.Pool     // pool of page buffers to avoid copying
}

// NewDecoder returns a new instance of Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:          r,
		zr:         lz4.NewReader(r),
		state:      stateHeader,
		hash:       crc64.New(crc64.MakeTable(crc64.ISO)),
		pageHasher: NewHasher(),
	}
}

// SetChecksumWorkers sets the number of parallel workers for checksum computation.
// If workers > 1, checksums will be computed in parallel.
// Must be called before DecodeHeader(). Default is 0 (sequential).
func (dec *Decoder) SetChecksumWorkers(workers int) {
	if workers < 0 {
		workers = 0
	}
	dec.checksumWorkers = workers
}

// startChecksumWorkers initializes the worker pool for parallel checksumming.
func (dec *Decoder) startChecksumWorkers() {
	if dec.checksumWorkers <= 0 {
		return // Sequential mode
	}

	dec.checksumJobs = make(chan checksumJob, dec.checksumWorkers*2)
	dec.checksumWorkerResults = make([]Checksum, dec.checksumWorkers)

	// Create buffer pool to avoid allocating new buffers for each page
	pageSize := int(dec.header.PageSize)
	dec.bufferPool = &sync.Pool{
		New: func() interface{} {
			buf := make([]byte, pageSize)
			return &buf
		},
	}

	for i := 0; i < dec.checksumWorkers; i++ {
		dec.checksumWg.Add(1)
		go dec.checksumWorker(i)
	}
}

// checksumWorker processes checksum jobs from the queue.
// Each worker maintains its own local XOR accumulator to avoid contention.
func (dec *Decoder) checksumWorker(workerID int) {
	defer dec.checksumWg.Done()

	hasher := NewHasher()
	localChecksum := Checksum(0)

	for job := range dec.checksumJobs {
		chksum := ChecksumPageWithHasher(hasher, job.pgno, job.data)
		localChecksum ^= chksum

		// Return buffer to pool
		dec.bufferPool.Put(&job.data)
	}

	// Store worker's accumulated checksum
	dec.checksumWorkerResults[workerID] = localChecksum
}

// stopChecksumWorkers waits for all workers to finish and combines their results.
func (dec *Decoder) stopChecksumWorkers() {
	if dec.checksumWorkers <= 0 {
		return
	}

	close(dec.checksumJobs)
	dec.checksumWg.Wait()

	// Combine all worker checksums (no synchronization needed, workers are done)
	for _, workerChecksum := range dec.checksumWorkerResults {
		dec.chksum = ChecksumFlag | (dec.chksum ^ workerChecksum)
	}
}

// N returns the number of bytes read.
func (dec *Decoder) N() int64 { return dec.n }

// PageN returns the number of pages read.
func (dec *Decoder) PageN() int { return dec.pageN }

// Header returns a copy of the header.
func (dec *Decoder) Header() Header { return dec.header }

// Trailer returns a copy of the trailer. File checksum available after Close().
func (dec *Decoder) Trailer() Trailer { return dec.trailer }

// PostApplyPos returns the replication position after underlying the LTX file is applied.
// Only valid after successful Close().
func (dec *Decoder) PostApplyPos() Pos {
	return Pos{
		TXID:              dec.header.MaxTXID,
		PostApplyChecksum: dec.trailer.PostApplyChecksum,
	}
}

// PageIndex returns a mapping of page numbers to byte offsets and sizes of those pages.
// This returns the raw reference and not a copy.
func (dec *Decoder) PageIndex() map[uint32]PageIndexElem {
	return dec.pageIndex
}

// Close verifies the reader is at the end of the file and that the checksum matches.
func (dec *Decoder) Close() error {
	if dec.state == stateClosed {
		return nil // no-op
	} else if dec.state != stateClose {
		return fmt.Errorf("cannot close, expected %s", dec.state)
	}

	// Wait for all checksum workers to finish if parallel mode
	dec.stopChecksumWorkers()

	// Slurp the remaining data in to memory so we can use the ByteReader interface.
	remainingBytes, err := io.ReadAll(dec.r)
	if err != nil {
		return fmt.Errorf("read all: %w", err)
	}
	remaining := bytes.NewReader(remainingBytes)

	// Write everything but the file checksum to the hash.
	dec.writeToHash(remainingBytes[:len(remainingBytes)-ChecksumSize])

	// Read page index.
	if dec.pageIndex, err = DecodePageIndex(remaining, 0, dec.header.MinTXID, dec.header.MaxTXID); err != nil {
		return fmt.Errorf("read page index: %w", err)
	}

	// Read trailer.
	b := make([]byte, TrailerSize)
	if _, err := io.ReadFull(remaining, b); err != nil {
		return err
	} else if err := dec.trailer.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal trailer: %w", err)
	}

	// TODO: Ensure last read page is equal to the commit for snapshot LTX files

	// Compare file checksum with checksum in trailer.
	if chksum := ChecksumFlag | Checksum(dec.hash.Sum64()); chksum != dec.trailer.FileChecksum {
		return ErrChecksumMismatch
	}

	// Verify post-apply checksum for snapshot files if checksums are being tracked.
	if dec.header.IsSnapshot() && !dec.header.NoChecksum() {
		if dec.trailer.PostApplyChecksum != dec.chksum {
			return fmt.Errorf("post-apply checksum in trailer (%s) does not match calculated checksum (%s)", dec.trailer.PostApplyChecksum, dec.chksum)
		}
	}

	// Update state to mark as closed.
	dec.state = stateClosed

	return nil
}

// DecodeHeader reads the LTX file header frame and stores it internally.
// Call Header() to retrieve the header after this is successfully called.
func (dec *Decoder) DecodeHeader() error {
	b := make([]byte, HeaderSize)
	if _, err := io.ReadFull(dec.r, b); err != nil {
		return err
	} else if err := dec.header.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal header: %w", err)
	}

	dec.writeToHash(b)
	dec.state = statePage

	if err := dec.header.Validate(); err != nil {
		return err
	}

	// Initialize checksum if checksum tracking is enabled.
	if !dec.header.NoChecksum() {
		dec.chksum = ChecksumFlag

		// Start parallel checksum workers if configured
		if dec.header.IsSnapshot() {
			dec.startChecksumWorkers()
		}
	}

	return nil
}

// DecodePage reads the next page header into hdr and associated page data.
func (dec *Decoder) DecodePage(hdr *PageHeader, data []byte) error {
	if dec.state == stateClosed {
		return ErrDecoderClosed
	} else if dec.state == stateClose {
		return io.EOF
	} else if dec.state != statePage {
		return fmt.Errorf("cannot read page header, expected %s", dec.state)
	} else if uint32(len(data)) != dec.header.PageSize {
		return fmt.Errorf("invalid page buffer size: %d, expecting %d", len(data), dec.header.PageSize)
	}

	// Read and unmarshal page header.
	b := make([]byte, PageHeaderSize)
	if _, err := io.ReadFull(dec.r, b); err != nil {
		return err
	} else if err := hdr.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	dec.writeToHash(b)

	// An empty page header indicates the end of the page block.
	if hdr.IsZero() {
		dec.state = stateClose
		return io.EOF
	}

	if err := hdr.Validate(); err != nil {
		return err
	}

	// Read page data next.
	dec.zr.Reset(dec.r)
	if _, err := io.ReadFull(dec.zr, data); err != nil {
		return err
	}
	dec.writeToHash(data)
	dec.pageN++

	// Read off the LZ4 trailer frame to ensure we hit EOF.
	if err := dec.readLZ4Trailer(); err != nil {
		return fmt.Errorf("read lz4 trailer: %w", err)
	}

	// Calculate checksum while decoding snapshots if tracking checksums.
	if dec.header.IsSnapshot() && !dec.header.NoChecksum() {
		if hdr.Pgno != LockPgno(dec.header.PageSize) {
			if dec.checksumWorkers > 0 {
				// Parallel mode: get buffer from pool, copy data, and send to worker
				bufPtr := dec.bufferPool.Get().(*[]byte)
				dataCopy := *bufPtr
				copy(dataCopy, data)
				dec.checksumJobs <- checksumJob{
					pgno: hdr.Pgno,
					data: dataCopy,
				}
			} else {
				// Sequential mode: compute checksum inline
				dec.chksum = ChecksumFlag | (dec.chksum ^ ChecksumPageWithHasher(dec.pageHasher, hdr.Pgno, data))
			}
		}
	}

	return nil
}

// Verify reads the entire file. Header & trailer can be accessed via methods
// after the file is successfully verified. All other data is discarded.
func (dec *Decoder) Verify() error {
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}

	var pageHeader PageHeader
	data := make([]byte, dec.header.PageSize)
	for i := 0; ; i++ {
		if err := dec.DecodePage(&pageHeader, data); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode page %d: %w", i, err)
		}
	}

	if err := dec.Close(); err != nil {
		return fmt.Errorf("close reader: %w", err)
	}
	return nil
}

// DecodeDatabaseTo decodes the LTX file as a SQLite database to w.
// The LTX file MUST be a snapshot file.
func (dec *Decoder) DecodeDatabaseTo(w io.Writer) error {
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}

	hdr := dec.Header()
	lockPgno := hdr.LockPgno()
	if !dec.header.IsSnapshot() {
		return fmt.Errorf("cannot decode non-snapshot LTX file to SQLite database")
	}

	var pageHeader PageHeader
	data := make([]byte, dec.header.PageSize)
	for pgno := uint32(1); pgno <= hdr.Commit; pgno++ {
		if pgno == lockPgno {
			// Write empty page for lock page.
			for i := range data {
				data[i] = 0
			}
		} else {
			// Otherwise read the page from the LTX decoder.
			if err := dec.DecodePage(&pageHeader, data); err != nil {
				return fmt.Errorf("decode page %d: %w", pgno, err)
			} else if pageHeader.Pgno != pgno {
				return fmt.Errorf("unexpected pgno while decoding page: read %d, expected %d", pageHeader.Pgno, pgno)
			}
		}

		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("write page %d: %w", pgno, err)
		}
	}

	// Issue one more final read and expect to see an EOF. This is required so
	// that the decoder can successfully close and validate.
	if err := dec.DecodePage(&pageHeader, data); err == nil {
		return fmt.Errorf("unexpected page %d after commit %d", pageHeader.Pgno, hdr.Commit)
	} else if err != io.EOF {
		return fmt.Errorf("unexpected error decoding after end of database: %w", err)
	}

	if err := dec.Close(); err != nil {
		return fmt.Errorf("close decoder: %w", err)
	}
	return nil
}

func (dec *Decoder) writeToHash(b []byte) {
	_, _ = dec.hash.Write(b)
	dec.n += int64(len(b))
}

// readLZ4Trailer reads the LZ4 trailer frame to ensure we hit EOF.
func (dec *Decoder) readLZ4Trailer() error {
	if _, err := io.ReadFull(dec.zr, make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("expected lz4 end frame")
	}
	return nil
}

// DecodeHeader decodes the header from r. Returns the header & read bytes.
func DecodeHeader(r io.Reader) (hdr Header, data []byte, err error) {
	data = make([]byte, HeaderSize)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return hdr, data[:n], err
	} else if err := hdr.UnmarshalBinary(data); err != nil {
		return hdr, data[:n], err
	}
	return hdr, data, nil
}

// DecodePageData decodes the page header & data from a single frame.
func DecodePageData(b []byte) (hdr PageHeader, data []byte, err error) {
	if err := hdr.UnmarshalBinary(b); err != nil {
		return hdr, data, fmt.Errorf("unmarshal: %w", err)
	}
	if hdr.IsZero() {
		return hdr, data, nil
	}

	zr := lz4.NewReader(bytes.NewReader(b[PageHeaderSize:]))
	data, err = io.ReadAll(zr)
	return hdr, data, err
}

// ParallelDecodeFile decodes all pages from an LTX file in parallel using the file path.
// Uses file seeks for random access - suitable for very large files without loading into memory.
// Returns a map of page number to page data.
func ParallelDecodeFile(filename string, workers int) (Header, map[uint32][]byte, error) {
	if workers <= 0 {
		workers = 2 // default
	}

	// Open file for reading
	f, err := os.Open(filename)
	if err != nil {
		return Header{}, nil, err
	}
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return Header{}, nil, err
	}
	fileSize := stat.Size()

	// Decode header
	headerBuf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		return Header{}, nil, err
	}
	hdr, _, err := DecodeHeader(bytes.NewReader(headerBuf))
	if err != nil {
		return Header{}, nil, fmt.Errorf("decode header: %w", err)
	}

	// Read the 8-byte index size field (located before the trailer)
	indexSizeOffset := fileSize - TrailerSize - 8
	indexSizeBuf := make([]byte, 8)
	if _, err := f.ReadAt(indexSizeBuf, indexSizeOffset); err != nil {
		return Header{}, nil, fmt.Errorf("read index size: %w", err)
	}

	// The size includes everything written by encodePageIndex EXCEPT the 8-byte size itself
	indexDataSize := int64(binary.BigEndian.Uint64(indexSizeBuf))

	// Calculate where the index data starts
	indexDataStart := indexSizeOffset - indexDataSize

	// Read index data + size field (DecodePageIndex expects both)
	indexBufSize := indexDataSize + 8
	indexBuf := make([]byte, indexBufSize)
	if _, err := f.ReadAt(indexBuf, indexDataStart); err != nil {
		return Header{}, nil, fmt.Errorf("read index: %w", err)
	}

	// Decode the index
	pageIndex, err := DecodePageIndex(bytes.NewReader(indexBuf), 0, hdr.MinTXID, hdr.MaxTXID)
	if err != nil {
		return Header{}, nil, fmt.Errorf("decode page index: %w", err)
	}

	// Create result map and job channel
	pages := make(map[uint32][]byte)
	var pagesMu sync.Mutex

	type decodeJob struct {
		pgno   uint32
		offset int64
		size   int64
	}
	jobs := make(chan decodeJob, len(pageIndex))

	// Start worker pool - each worker opens its own file handle
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each worker gets its own file handle for concurrent reads
			workerFile, err := os.Open(filename)
			if err != nil {
				return
			}
			defer workerFile.Close()

			for job := range jobs {
				// Read page data at offset
				pageData := make([]byte, job.size)
				if _, err := workerFile.ReadAt(pageData, job.offset); err != nil {
					continue
				}

				// Decode page
				_, data, err := DecodePageData(pageData)
				if err != nil {
					continue
				}

				pagesMu.Lock()
				pages[job.pgno] = data
				pagesMu.Unlock()
			}
		}()
	}

	// Send jobs
	for pgno, elem := range pageIndex {
		jobs <- decodeJob{
			pgno:   pgno,
			offset: elem.Offset,
			size:   elem.Size,
		}
	}
	close(jobs)

	// Wait for completion
	wg.Wait()

	return hdr, pages, nil
}

// DecodePageIndex decodes the page index from r.
func DecodePageIndex(r io.ByteReader, level int, minTXID, maxTXID TXID) (map[uint32]PageIndexElem, error) {
	pageIndex := make(map[uint32]PageIndexElem)

	for {
		pgno, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read page index pgno: %w", err)
		} else if pgno == 0 {
			break // End when we hit the end marker.
		}

		offset, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read page index offset: %w", err)
		}
		size, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read page index size: %w", err)
		}

		pageIndex[uint32(pgno)] = PageIndexElem{
			Level:   level,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
			Offset:  int64(offset),
			Size:    int64(size),
		}
	}

	// Read size of page index.
	var size uint64
	if err := binary.Read(r.(io.Reader), binary.BigEndian, &size); err != nil {
		return nil, fmt.Errorf("read page index size: %w", err)
	}

	return pageIndex, nil
}

// SeekableDecoder provides on-demand page decoding from an LTX file.
// Unlike ParallelDecodeFile which loads all pages into memory, SeekableDecoder
// allows decoding individual pages or batches as needed, minimizing memory usage.
// It uses parallel workers for concurrent page decompression when multiple pages are requested.
type SeekableDecoder struct {
	f       *os.File
	header  Header
	trailer Trailer
	index   map[uint32]PageIndexElem
	workers int

	// Buffer pool for efficient memory reuse
	bufferPool *sync.Pool
}

// NewSeekableDecoder creates a new seekable decoder for the given LTX file.
// workers specifies the number of parallel decompression workers (default: 4 if workers <= 0).
func NewSeekableDecoder(filename string, workers int) (*SeekableDecoder, error) {
	if workers <= 0 {
		workers = 4 // default
	}

	// Open file
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := stat.Size()

	// Read header
	headerBuf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		return nil, err
	}
	hdr, _, err := DecodeHeader(bytes.NewReader(headerBuf))
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("decode header: %w", err)
	}

	// Read trailer
	trailerBuf := make([]byte, TrailerSize)
	if _, err := f.ReadAt(trailerBuf, fileSize-TrailerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("read trailer: %w", err)
	}
	var trailer Trailer
	if err := trailer.UnmarshalBinary(trailerBuf); err != nil {
		f.Close()
		return nil, fmt.Errorf("unmarshal trailer: %w", err)
	}

	// Read page index from end of file
	indexSizeOffset := fileSize - TrailerSize - 8
	indexSizeBuf := make([]byte, 8)
	if _, err := f.ReadAt(indexSizeBuf, indexSizeOffset); err != nil {
		f.Close()
		return nil, fmt.Errorf("read index size: %w", err)
	}

	indexDataSize := int64(binary.BigEndian.Uint64(indexSizeBuf))
	indexDataStart := indexSizeOffset - indexDataSize
	indexBufSize := indexDataSize + 8
	indexBuf := make([]byte, indexBufSize)
	if _, err := f.ReadAt(indexBuf, indexDataStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("read index: %w", err)
	}

	pageIndex, err := DecodePageIndex(bytes.NewReader(indexBuf), 0, hdr.MinTXID, hdr.MaxTXID)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("decode page index: %w", err)
	}

	sd := &SeekableDecoder{
		f:       f,
		header:  hdr,
		trailer: trailer,
		index:   pageIndex,
		workers: workers,
	}

	// Initialize buffer pool for page data
	sd.bufferPool = &sync.Pool{
		New: func() interface{} {
			return new([]byte)
		},
	}

	return sd, nil
}

// Header returns a copy of the file header.
func (sd *SeekableDecoder) Header() Header { return sd.header }

// Trailer returns a copy of the file trailer.
func (sd *SeekableDecoder) Trailer() Trailer { return sd.trailer }

// PageIndex returns the page index mapping page numbers to file offsets.
func (sd *SeekableDecoder) PageIndex() map[uint32]PageIndexElem { return sd.index }

// Close closes the underlying file.
func (sd *SeekableDecoder) Close() error {
	if sd.f != nil {
		return sd.f.Close()
	}
	return nil
}

// DecodePage decodes a single page by page number.
// Returns an error if the page is not in the index.
func (sd *SeekableDecoder) DecodePage(pgno uint32) ([]byte, error) {
	elem, ok := sd.index[pgno]
	if !ok {
		return nil, fmt.Errorf("page %d not found in index", pgno)
	}

	// Read page data at offset
	pageData := make([]byte, elem.Size)
	if _, err := sd.f.ReadAt(pageData, elem.Offset); err != nil {
		return nil, fmt.Errorf("read page %d: %w", pgno, err)
	}

	// Decode page
	_, data, err := DecodePageData(pageData)
	if err != nil {
		return nil, fmt.Errorf("decode page %d: %w", pgno, err)
	}

	return data, nil
}

// DecodePages decodes multiple pages in parallel.
// Pages not found in the index are skipped (not included in result map).
func (sd *SeekableDecoder) DecodePages(pgnos []uint32) (map[uint32][]byte, error) {
	if len(pgnos) == 0 {
		return make(map[uint32][]byte), nil
	}

	// Result map
	pages := make(map[uint32][]byte, len(pgnos))
	var pagesMu sync.Mutex
	var resultErr error
	var errMu sync.Mutex

	// Create job channel
	type decodeJob struct {
		pgno   uint32
		offset int64
		size   int64
	}
	jobs := make(chan decodeJob, len(pgnos))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < sd.workers && i < len(pgnos); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each worker opens its own file handle for concurrent reads
			workerFile, err := os.Open(sd.f.Name())
			if err != nil {
				errMu.Lock()
				if resultErr == nil {
					resultErr = err
				}
				errMu.Unlock()
				return
			}
			defer workerFile.Close()

			for job := range jobs {
				// Read page data at offset
				pageData := make([]byte, job.size)
				if _, err := workerFile.ReadAt(pageData, job.offset); err != nil {
					errMu.Lock()
					if resultErr == nil {
						resultErr = fmt.Errorf("read page %d: %w", job.pgno, err)
					}
					errMu.Unlock()
					continue
				}

				// Decode page
				_, data, err := DecodePageData(pageData)
				if err != nil {
					errMu.Lock()
					if resultErr == nil {
						resultErr = fmt.Errorf("decode page %d: %w", job.pgno, err)
					}
					errMu.Unlock()
					continue
				}

				pagesMu.Lock()
				pages[job.pgno] = data
				pagesMu.Unlock()
			}
		}()
	}

	// Send jobs for pages that exist in the index
	for _, pgno := range pgnos {
		if elem, ok := sd.index[pgno]; ok {
			jobs <- decodeJob{
				pgno:   pgno,
				offset: elem.Offset,
				size:   elem.Size,
			}
		}
	}
	close(jobs)

	// Wait for completion
	wg.Wait()

	return pages, resultErr
}

// DecodeAllPages decodes all pages in the index in parallel.
// WARNING: Peak memory usage is unbounded and scales with file size.
// Typically requires 2x+ the size of the database (e.g., 200MB peak for 100MB file).
// For large files, use DecodeInBatches() instead to limit memory usage.
func (sd *SeekableDecoder) DecodeAllPages() (map[uint32][]byte, error) {
	pgnos := make([]uint32, 0, len(sd.index))
	for pgno := range sd.index {
		pgnos = append(pgnos, pgno)
	}
	return sd.DecodePages(pgnos)
}

// DecodeInBatches decodes all pages in batches, calling fn for each batch.
// This allows processing large files without keeping all pages in memory.
// Default batch size is 800 pages (~25MB for 32KB pages).
// Returns on first error from fn or decoding error.
func (sd *SeekableDecoder) DecodeInBatches(fn func(pages map[uint32][]byte) error) error {
	return sd.DecodeInBatchesWithSize(800, fn)
}

// DecodeInBatchesWithSize decodes all pages in batches of the specified size.
// Calls fn for each batch, allowing processing without keeping all pages in memory.
func (sd *SeekableDecoder) DecodeInBatchesWithSize(batchSize int, fn func(pages map[uint32][]byte) error) error {
	if batchSize <= 0 {
		batchSize = 800
	}

	// Get all page numbers and sort them
	pgnos := make([]uint32, 0, len(sd.index))
	for pgno := range sd.index {
		pgnos = append(pgnos, pgno)
	}

	// Sort for sequential processing
	type uint32Slice []uint32
	sort.Slice(pgnos, func(i, j int) bool { return pgnos[i] < pgnos[j] })

	// Process in batches
	for i := 0; i < len(pgnos); i += batchSize {
		end := i + batchSize
		if end > len(pgnos) {
			end = len(pgnos)
		}

		batch := pgnos[i:end]
		pages, err := sd.DecodePages(batch)
		if err != nil {
			return err
		}

		if err := fn(pages); err != nil {
			return err
		}
	}

	return nil
}
