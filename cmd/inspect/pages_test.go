package inspect

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// tag builds a value of the repeated column in testdata/repeated-row-group.parquet.
func tag(row, index int) string {
	return fmt.Sprintf("tag-%d-%d%s", row, index, strings.Repeat("-pad", 8))
}

// Expected values of every page of the repeated "tags" column, per row group.
// Row i holds i%3+1 tags, so a page's value count is not its row count.
var repeatedTagPages = [][][]any{
	{
		{tag(0, 0), tag(1, 0), tag(1, 1)},
		{tag(2, 0), tag(2, 1), tag(2, 2)},
		{tag(3, 0), tag(4, 0), tag(4, 1)},
	},
	{
		{tag(5, 0), tag(5, 1), tag(5, 2)},
		{tag(6, 0), tag(7, 0), tag(7, 1)},
		{tag(8, 0), tag(8, 1), tag(8, 2)},
	},
	{
		{tag(9, 0), tag(10, 0), tag(10, 1)},
		{tag(11, 0), tag(11, 1), tag(11, 2)},
	},
}

// pageReadInput gathers readPageValues' inputs, so a test can step in first.
func pageReadInput(t *testing.T, pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex int) (*parquet.ColumnChunk, *pschema.SchemaNode, []PageInfo) {
	t.Helper()

	col := pr.Footer.RowGroups[rowGroupIndex].Columns[columnChunkIndex]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
	schemaRoot, err := pschema.NewSchemaTree(context.Background(), pr, pschema.SchemaOption{SkipPageEncoding: true})
	require.NoError(t, err)

	pages, err := Cmd{}.readPages(context.Background(), pr, rowGroupIndex, columnChunkIndex, schemaRoot.GetPathMap()[pathKey])
	require.NoError(t, err)

	return col, schemaRoot.GetPathMap()[pathKey], pages
}

func pageValues(t *testing.T, pr *reader.ParquetReader, rowGroupIndex, columnChunkIndex, pageIndex int) []any {
	t.Helper()

	col, schemaNode, pages := pageReadInput(t, pr, rowGroupIndex, columnChunkIndex)
	values, err := Cmd{}.readPageValues(context.Background(), pr, rowGroupIndex, columnChunkIndex, col, schemaNode, pages, pageIndex)
	require.NoError(t, err)
	return values
}

func TestReadPageValuesRepeatedColumn(t *testing.T) {
	for rowGroupIndex, wantPages := range repeatedTagPages {
		for pageIndex, want := range wantPages {
			t.Run(fmt.Sprintf("rg%d-page%d", rowGroupIndex, pageIndex), func(t *testing.T) {
				pr, err := pio.NewParquetFileReader(context.Background(), "../../testdata/repeated-row-group.parquet", pio.ReadOption{})
				require.NoError(t, err)
				defer func() { _ = pr.PFile.Close() }()

				require.Equal(t, want, pageValues(t, pr, rowGroupIndex, 1, pageIndex))
			})
		}
	}
}

// Without an offset index a repeated column's page is cut out by value count.
// The fixture's pages still start on rows: parquet-go never ends one mid-row,
// so TestPageValueSlice covers that layout instead.
func TestReadPageValuesWithoutOffsetIndex(t *testing.T) {
	for rowGroupIndex, wantPages := range repeatedTagPages {
		for pageIndex, want := range wantPages {
			t.Run(fmt.Sprintf("rg%d-page%d", rowGroupIndex, pageIndex), func(t *testing.T) {
				pr, err := pio.NewParquetFileReader(context.Background(), "../../testdata/repeated-row-group.parquet", pio.ReadOption{})
				require.NoError(t, err)
				defer func() { _ = pr.PFile.Close() }()

				for _, rowGroup := range pr.Footer.RowGroups {
					rowGroup.Columns[1].OffsetIndexOffset = nil
					rowGroup.Columns[1].OffsetIndexLength = nil
				}

				require.Equal(t, want, pageValues(t, pr, rowGroupIndex, 1, pageIndex))
			})
		}
	}
}

func TestReadPageValuesReadsOnlyTargetPage(t *testing.T) {
	pr, err := pio.NewParquetFileReader(context.Background(), "../../testdata/repeated-row-group.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pr.PFile.Close() }()

	// The last row group's last page: everything before it used to be read too.
	rowGroupIndex := len(pr.Footer.RowGroups) - 1
	pageIndex := len(repeatedTagPages[rowGroupIndex]) - 1
	col, schemaNode, pages := pageReadInput(t, pr, rowGroupIndex, 1)

	log := &readLog{}
	pr.PFile = &recordingFile{inner: pr.PFile, log: log}
	// The reader opened a buffer per column up front, each cloning the file it
	// was built from; drop them so the read goes through the recording wrapper.
	require.NoError(t, pr.ReadStopWithContext(context.Background()))
	clear(pr.ColumnBuffers)

	values, err := Cmd{}.readPageValues(context.Background(), pr, rowGroupIndex, 1, col, schemaNode, pages, pageIndex)
	require.NoError(t, err)
	require.Equal(t, repeatedTagPages[rowGroupIndex][pageIndex], values)
	require.NotEmpty(t, log.reads, "no reads recorded, the wrapper is not in the path")

	for i, rowGroup := range pr.Footer.RowGroups {
		start, end := columnChunkExtent(rowGroup.Columns[1].MetaData)
		if i != rowGroupIndex {
			require.False(t, log.touches(start, end), "read row group %d column data at [%d, %d)", i, start, end)
			continue
		}
		// Within the target row group, the data pages before the target are
		// skipped over rather than read. A dictionary page, if the fixture ever
		// gains one, has to be read to decode the target, so start at the first
		// data page instead of at the chunk.
		firstDataPage := pages[0].Offset
		for _, page := range pages {
			if page.Type == parquet.PageType_DATA_PAGE || page.Type == parquet.PageType_DATA_PAGE_V2 {
				firstDataPage = page.Offset
				break
			}
		}
		require.Less(t, firstDataPage, pages[pageIndex].Offset, "target page must have pages before it")
		require.False(t, log.touches(firstDataPage, pages[pageIndex].Offset), "read data pages before the target page")
	}
}

func columnChunkExtent(meta *parquet.ColumnMetaData) (int64, int64) {
	start := meta.DataPageOffset
	if meta.DictionaryPageOffset != nil && *meta.DictionaryPageOffset < start {
		start = *meta.DictionaryPageOffset
	}
	return start, start + meta.TotalCompressedSize
}

func TestSkipToPageHonorsCanceledContext(t *testing.T) {
	pr, err := pio.NewParquetFileReader(context.Background(), "../../testdata/repeated-row-group.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pr.PFile.Close() }()

	// Row group 1 onwards has rows to skip before the page is reached.
	col, schemaNode, pages := pageReadInput(t, pr, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = Cmd{}.readPageValues(ctx, pr, 1, 1, col, schemaNode, pages, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "failed to skip to page")
}

func TestOffsetIndexRowRange(t *testing.T) {
	location := func(offset, firstRow int64) *parquet.PageLocation {
		return &parquet.PageLocation{Offset: offset, FirstRowIndex: firstRow}
	}

	testCases := map[string]struct {
		locations    []*parquet.PageLocation
		pageOffset   int64
		rowGroupRows int64
		firstRow     int64
		numRows      int64
		ok           bool
	}{
		"first-page":    {[]*parquet.PageLocation{location(4, 0), location(60, 3)}, 4, 5, 0, 3, true},
		"last-page":     {[]*parquet.PageLocation{location(4, 0), location(60, 3)}, 60, 5, 3, 2, true},
		"skips-nil":     {[]*parquet.PageLocation{nil, location(60, 0)}, 60, 5, 0, 5, true},
		"unknown-page":  {[]*parquet.PageLocation{location(4, 0)}, 99, 5, 0, 0, false},
		"no-locations":  {nil, 4, 5, 0, 0, false},
		"row-overrun":   {[]*parquet.PageLocation{location(4, 0), location(60, 9)}, 4, 5, 0, 0, false},
		"empty-range":   {[]*parquet.PageLocation{location(4, 3), location(60, 3)}, 4, 5, 0, 0, false},
		"negative-row":  {[]*parquet.PageLocation{location(4, -1)}, 4, 5, 0, 0, false},
		"single-page":   {[]*parquet.PageLocation{location(4, 0)}, 4, 5, 0, 5, true},
		"trailing-nil":  {[]*parquet.PageLocation{location(4, 0), nil}, 4, 5, 0, 5, true},
		"zero-row-file": {[]*parquet.PageLocation{location(4, 0)}, 4, 0, 0, 0, false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			firstRow, numRows, ok := offsetIndexRowRange(tc.locations, tc.pageOffset, tc.rowGroupRows)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.firstRow, firstRow)
			require.Equal(t, tc.numRows, numRows)
		})
	}
}

func TestColumnHasRepetition(t *testing.T) {
	pr, err := pio.NewParquetFileReader(context.Background(), "../../testdata/repeated-row-group.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pr.PFile.Close() }()

	require.False(t, columnHasRepetition(pr, 0), "id is not repeated")
	require.True(t, columnHasRepetition(pr, 1), "tags is repeated")
	require.True(t, columnHasRepetition(pr, -1), "unknown column")
	require.True(t, columnHasRepetition(pr, 99), "unknown column")
	require.True(t, columnHasRepetition(&reader.ParquetReader{}, 0), "no schema handler")
}

func TestPageValueSlice(t *testing.T) {
	numValues := func(count int32) *int32 { return &count }
	pages := []PageInfo{
		{Type: parquet.PageType_DATA_PAGE},
		{Type: parquet.PageType_DICTIONARY_PAGE, NumValues: numValues(4)},
		{Type: parquet.PageType_DATA_PAGE, NumValues: numValues(2)},
		{Type: parquet.PageType_DATA_PAGE, NumValues: numValues(2)},
	}
	values := []any{"a", "b", "c"}

	require.Equal(t, []any{"a", "b"}, pageValueSlice(values, pages, 2))
	// The row group holds fewer values than the headers claim, so the tail is short.
	require.Equal(t, []any{"c"}, pageValueSlice(values, pages, 3))
	require.Equal(t, []any{}, pageValueSlice(values[:2], pages, 3))

	// A writer that does not maintain a page index may end a page mid-row.
	// Slicing by value count does not care where the row boundaries fall.
	midRow := []PageInfo{
		{Type: parquet.PageType_DATA_PAGE, NumValues: numValues(4)},
		{Type: parquet.PageType_DATA_PAGE, NumValues: numValues(2)},
	}
	rowGroup := []any{"a", "b", "c", "d", "e", "f"} // rows ["a" "b"] ["c" "d" "e" "f"]
	require.Equal(t, []any{"a", "b", "c", "d"}, pageValueSlice(rowGroup, midRow, 0))
	require.Equal(t, []any{"e", "f"}, pageValueSlice(rowGroup, midRow, 1))
}

type readLog struct {
	mu    sync.Mutex
	reads [][2]int64
}

func (l *readLog) add(start, end int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reads = append(l.reads, [2]int64{start, end})
}

func (l *readLog) touches(start, end int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, read := range l.reads {
		if read[0] < end && start < read[1] {
			return true
		}
	}
	return false
}

// recordingFile logs every byte range read, clones for column buffers included.
type recordingFile struct {
	inner source.ParquetFileReader
	log   *readLog
	pos   int64
}

func (f *recordingFile) Read(p []byte) (int, error) {
	n, err := f.inner.Read(p)
	if n > 0 {
		f.log.add(f.pos, f.pos+int64(n))
		f.pos += int64(n)
	}
	return n, err
}

func (f *recordingFile) Seek(offset int64, whence int) (int64, error) {
	pos, err := f.inner.Seek(offset, whence)
	if err == nil {
		f.pos = pos
	}
	return pos, err
}

func (f *recordingFile) Close() error { return f.inner.Close() }

func (f *recordingFile) Open(name string) (source.ParquetFileReader, error) {
	inner, err := f.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &recordingFile{inner: inner, log: f.log}, nil
}

func (f *recordingFile) Clone() (source.ParquetFileReader, error) {
	inner, err := f.inner.Clone()
	if err != nil {
		return nil, err
	}
	return &recordingFile{inner: inner, log: f.log}, nil
}

func TestConvertIndexPageHeader(t *testing.T) {
	page := (Cmd{}).convertPageHeaderInfo(reader.PageHeaderInfo{PageType: parquet.PageType_INDEX_PAGE}, nil)
	if page.Note != "Index page (column index)" {
		t.Fatalf("convertPageHeaderInfo() note = %q", page.Note)
	}
}

func TestPageReadsHonorCanceledContext(t *testing.T) {
	pr, err := pio.NewParquetFileReader(context.Background(), "../../testdata/dict-page.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pr.PFile.Close() }()

	col := pr.Footer.RowGroups[0].Columns[0]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
	schemaRoot, err := pschema.NewSchemaTree(context.Background(), pr, pschema.SchemaOption{SkipPageEncoding: true})
	require.NoError(t, err)
	schemaNode := schemaRoot.GetPathMap()[pathKey]

	pages, err := (Cmd{}).readPages(context.Background(), pr, 0, 0, schemaNode)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(pages), 2)
	require.Equal(t, parquet.PageType_DICTIONARY_PAGE, pages[0].Type)

	dataPageIndex := -1
	for i, page := range pages {
		if page.Type == parquet.PageType_DATA_PAGE || page.Type == parquet.PageType_DATA_PAGE_V2 {
			dataPageIndex = i
			break
		}
	}
	require.NotEqual(t, -1, dataPageIndex)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := map[string]func() error{
		"page headers": func() error {
			_, err := (Cmd{}).readPages(ctx, pr, 0, 0, schemaNode)
			return err
		},
		"dictionary values": func() error {
			_, err := (Cmd{}).readDictionaryPageValues(ctx, pr, col, schemaNode, pages[0])
			return err
		},
		"column values": func() error {
			_, err := (Cmd{}).readPageValues(ctx, pr, 0, 0, col, schemaNode, pages, dataPageIndex)
			return err
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test()
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
