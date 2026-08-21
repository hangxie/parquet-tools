package schema

import (
	"context"
	"testing"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	pqschema "github.com/hangxie/parquet-go/v3/schema"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestSchemaOptionZeroValue(t *testing.T) {
	option := SchemaOption{}
	if option.FailOnInt96 || option.SkipPageEncoding || option.SkipBloomFilter {
		t.Fatal("zero-value schema option must leave optional behavior disabled")
	}
}

func TestBuildEncodingMapEmptyColumnChunk(t *testing.T) {
	// A nil PFile proves no read is attempted for an empty column chunk.
	newColumn := func(path string) *parquet.ColumnChunk {
		return &parquet.ColumnChunk{
			MetaData: &parquet.ColumnMetaData{PathInSchema: []string{path}},
		}
	}

	pr := &reader.ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{newColumn("a"), newColumn("b")}},
			},
		},
	}

	got, err := buildEncodingMap(context.Background(), pr)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestBuildEncodingMapEmptyFirstRowGroup(t *testing.T) {
	// A column empty in row group 0 but not later still has an encoding to
	// report; omitting it would leave transcode writing the default instead.
	pr, err := pio.NewParquetFileReader(context.Background(), "../testdata/row-group.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()
	require.Greater(t, len(pr.Footer.RowGroups), 1, "fixture must have more than one row group")

	want, err := buildEncodingMap(context.Background(), pr)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	// The offsets go too: left in place, a read aimed at row group 0 still
	// succeeds and returns the same encodings, hiding the fix instead of pinning it.
	for _, chunk := range pr.Footer.RowGroups[0].Columns {
		chunk.MetaData.NumValues = 0
		chunk.MetaData.DataPageOffset = 0
		chunk.MetaData.DictionaryPageOffset = nil
	}

	got, err := buildEncodingMap(context.Background(), pr)
	require.NoError(t, err)
	require.Equal(t, want, got, "encoding should come from the first row group holding values")
}

func TestBuildMapsSkipChunkWithoutMetadata(t *testing.T) {
	// A chunk without metadata names no column path. Dereferencing it panicked
	// inside the errgroup goroutine, taking the process down with it.
	pr := &reader.ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{nil, {}}},
			},
		},
		SchemaHandler: &pqschema.SchemaHandler{},
	}

	require.NotPanics(t, func() {
		got, err := buildEncodingMap(context.Background(), pr)
		require.NoError(t, err)
		require.Empty(t, got)
	})
	require.NotPanics(t, func() { require.Empty(t, buildCompressionCodecMap(pr)) })
	require.NotPanics(t, func() {
		// No chunk carries a filter offset, so nothing is sized and no read is attempted.
		got, err := buildBloomFilterMap(context.Background(), pr)
		require.NoError(t, err)
		require.Empty(t, got)
	})
}

func TestBuildMapsUseTheRowGroupHoldingValues(t *testing.T) {
	// Row group 0 is empty; row group 1 holds the real codec and a filter. Reading
	// row group 0 reports the wrong codec and hides the filter.
	offset := int64(2048)
	emptyChunk := &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
		PathInSchema: []string{"col"},
		NumValues:    0,
		Codec:        parquet.CompressionCodec_UNCOMPRESSED,
	}}
	populatedChunk := &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
		PathInSchema:      []string{"col"},
		NumValues:         42,
		Codec:             parquet.CompressionCodec_ZSTD,
		BloomFilterOffset: &offset,
	}}

	pr := &reader.ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{emptyChunk}},
				{Columns: []*parquet.ColumnChunk{populatedChunk}},
			},
		},
		SchemaHandler: &pqschema.SchemaHandler{
			Infos:    []*common.Tag{{InName: "Root"}, {InName: "Col"}},
			MapIndex: map[string]int32{common.PathToStr([]string{"Root", "col"}): 1},
		},
	}

	require.Equal(t, map[string]string{"col": "ZSTD"}, buildCompressionCodecMap(pr))
	// The filter lives in row group 1, and that is the row group it must be sized from.
	rgIndex, chunk, ok := firstChunkWithBloomFilter(pr.Footer.RowGroups, 0)
	require.True(t, ok)
	require.Equal(t, 1, rgIndex)
	require.Same(t, populatedChunk, chunk)
}

func TestFirstChunkWithBloomFilter(t *testing.T) {
	// Values in row group 0 with no filter, a filter in row group 2: the first
	// populated chunk alone would miss it, and the size must come from where it is.
	offset := int64(4096)
	chunk := func(numValues int64, bloom *int64) *parquet.ColumnChunk {
		return &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
			PathInSchema:      []string{"col"},
			NumValues:         numValues,
			BloomFilterOffset: bloom,
		}}
	}
	filtered := chunk(10, &offset)

	rowGroups := []*parquet.RowGroup{
		{Columns: []*parquet.ColumnChunk{chunk(10, nil)}},
		// A row group short of this column must be stepped over, not indexed.
		{Columns: []*parquet.ColumnChunk{}},
		{Columns: []*parquet.ColumnChunk{filtered}},
	}

	rgIndex, got, ok := firstChunkWithBloomFilter(rowGroups, 0)
	require.True(t, ok)
	require.Equal(t, 2, rgIndex)
	require.Same(t, filtered, got)

	// A nil chunk and one without metadata are skipped rather than dereferenced.
	_, _, ok = firstChunkWithBloomFilter([]*parquet.RowGroup{
		{Columns: []*parquet.ColumnChunk{nil}},
		{Columns: []*parquet.ColumnChunk{{}}},
		{Columns: []*parquet.ColumnChunk{chunk(10, nil)}},
	}, 0)
	require.False(t, ok, "no chunk carries a filter offset")
}

func TestCompressionCodecUsesFirstPopulatedChunk(t *testing.T) {
	// A schema leaf holds one codec, so the first populated chunk is
	// representative by design. Pinned so it reads as a decision.
	chunk := func(numValues int64, codec parquet.CompressionCodec) *parquet.ColumnChunk {
		return &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
			PathInSchema: []string{"col"},
			NumValues:    numValues,
			Codec:        codec,
		}}
	}

	forRowGroups := func(rowGroups ...*parquet.RowGroup) *reader.ParquetReader {
		return &reader.ParquetReader{Footer: &parquet.FileMetaData{RowGroups: rowGroups}}
	}

	pr := forRowGroups(
		&parquet.RowGroup{Columns: []*parquet.ColumnChunk{chunk(0, parquet.CompressionCodec_UNCOMPRESSED)}},
		&parquet.RowGroup{Columns: []*parquet.ColumnChunk{chunk(10, parquet.CompressionCodec_GZIP)}},
		&parquet.RowGroup{Columns: []*parquet.ColumnChunk{chunk(10, parquet.CompressionCodec_ZSTD)}},
	)
	require.Equal(t, map[string]string{"col": "GZIP"}, buildCompressionCodecMap(pr),
		"the first populated chunk stands for the column, not the empty one and not the last")

	// With nothing populated there is no representative, so the column's own
	// chunk is read even though no data was written with that codec.
	empty := forRowGroups(
		&parquet.RowGroup{Columns: []*parquet.ColumnChunk{chunk(0, parquet.CompressionCodec_SNAPPY)}},
		&parquet.RowGroup{Columns: []*parquet.ColumnChunk{chunk(0, parquet.CompressionCodec_ZSTD)}},
	)
	require.Equal(t, map[string]string{"col": "SNAPPY"}, buildCompressionCodecMap(empty))
}

func TestFirstNonEmptyChunk(t *testing.T) {
	chunk := func(numValues int64) *parquet.ColumnChunk {
		return &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{NumValues: numValues}}
	}
	rowGroup := func(chunks ...*parquet.ColumnChunk) *parquet.RowGroup {
		return &parquet.RowGroup{Columns: chunks}
	}

	tests := []struct {
		name        string
		rowGroups   []*parquet.RowGroup
		colIndex    int
		wantRGIndex int
		wantOK      bool
	}{
		{name: "no row groups", colIndex: 0},
		{
			name:      "values in the first row group",
			rowGroups: []*parquet.RowGroup{rowGroup(chunk(5)), rowGroup(chunk(5))},
			colIndex:  0, wantRGIndex: 0, wantOK: true,
		},
		{
			name:      "first row group empty, second holds values",
			rowGroups: []*parquet.RowGroup{rowGroup(chunk(0)), rowGroup(chunk(5))},
			colIndex:  0, wantRGIndex: 1, wantOK: true,
		},
		{
			name:      "only the last row group holds values",
			rowGroups: []*parquet.RowGroup{rowGroup(chunk(0)), rowGroup(chunk(0)), rowGroup(chunk(3))},
			colIndex:  0, wantRGIndex: 2, wantOK: true,
		},
		{
			name:      "empty in every row group",
			rowGroups: []*parquet.RowGroup{rowGroup(chunk(0)), rowGroup(chunk(0))},
			colIndex:  0,
		},
		{
			name:      "columns are tracked independently",
			rowGroups: []*parquet.RowGroup{rowGroup(chunk(5), chunk(0)), rowGroup(chunk(5), chunk(7))},
			colIndex:  1, wantRGIndex: 1, wantOK: true,
		},
		{
			name:      "column index past the row group",
			rowGroups: []*parquet.RowGroup{rowGroup(chunk(5))},
			colIndex:  3,
		},
		{
			name:      "nil chunk and nil metadata are skipped",
			rowGroups: []*parquet.RowGroup{rowGroup(nil), rowGroup(&parquet.ColumnChunk{}), rowGroup(chunk(2))},
			colIndex:  0, wantRGIndex: 2, wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRGIndex, gotChunk, gotOK := firstNonEmptyChunk(tt.rowGroups, tt.colIndex)
			require.Equal(t, tt.wantOK, gotOK)
			require.Equal(t, tt.wantRGIndex, gotRGIndex)
			if tt.wantOK {
				require.NotNil(t, gotChunk)
			} else {
				require.Nil(t, gotChunk)
			}
		})
	}
}

func TestBuildBloomFilterMap(t *testing.T) {
	// Same keys testdata/gen/encrypted-bloom-filter writes the fixture with.
	encFooterKey := "MDEyMzQ1Njc4OTAxMjM0NQ=="
	encColumnKey := "MTIzNDU2Nzg5MDEyMzQ1MA=="

	testCases := []struct {
		name     string
		uri      string
		option   pio.ReadOption
		expected map[string]bloomFilterInfo
	}{
		{
			name:     "empty row groups",
			uri:      "../testdata/empty.parquet",
			expected: map[string]bloomFilterInfo{},
		},
		{
			name:     "file without bloom filters",
			uri:      "../testdata/good.parquet",
			expected: map[string]bloomFilterInfo{},
		},
		{
			name: "file with bloom filters",
			uri:  "../testdata/bloom-filter.parquet",
			expected: map[string]bloomFilterInfo{
				"ID":    {Enabled: true, Size: 1024},
				"Name":  {Enabled: true, Size: 4096},
				"Score": {Enabled: true, Size: 1024},
			},
		},
		{
			// ID and Footer are encrypted with keys nobody supplied, but the footer
			// still names their filters, so both stay enabled and lose only the size.
			name: "no key sizes the plaintext column alone",
			uri:  "../testdata/encrypted-bloom-filter.parquet",
			expected: map[string]bloomFilterInfo{
				"ID":     {Enabled: true},
				"Name":   {Enabled: true, Size: 4096},
				"Footer": {Enabled: true},
			},
		},
		{
			// Footer needs the footer key rather than its own, which is the second
			// of the two shapes ErrColumnKeyRequired covers.
			name:   "footer key sizes the footer-key column, ID still degrades",
			uri:    "../testdata/encrypted-bloom-filter.parquet",
			option: pio.ReadOption{FooterKey: &encFooterKey},
			expected: map[string]bloomFilterInfo{
				"ID":     {Enabled: true},
				"Name":   {Enabled: true, Size: 4096},
				"Footer": {Enabled: true, Size: 2048},
			},
		},
		{
			// With every key at hand nothing degrades, so transcode carries the
			// configured sizes over instead of the writer default.
			name:   "every key sizes every filter",
			uri:    "../testdata/encrypted-bloom-filter.parquet",
			option: pio.ReadOption{FooterKey: &encFooterKey, ColumnKeys: []string{"ID=" + encColumnKey}},
			expected: map[string]bloomFilterInfo{
				"ID":     {Enabled: true, Size: 1024},
				"Name":   {Enabled: true, Size: 4096},
				"Footer": {Enabled: true, Size: 2048},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pr, err := pio.NewParquetFileReader(context.Background(), tc.uri, tc.option)
			require.NoError(t, err)
			defer func() {
				_ = pr.PFile.Close()
			}()

			result, err := buildBloomFilterMap(context.Background(), pr)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestNewSchemaTreeSkipBloomFilter(t *testing.T) {
	testCases := map[string]struct {
		option      SchemaOption
		wantFilters bool
	}{
		"default":           {option: SchemaOption{}, wantFilters: true},
		"skip-bloom-filter": {option: SchemaOption{SkipBloomFilter: true}, wantFilters: false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pr, err := pio.NewParquetFileReader(context.Background(), "../testdata/bloom-filter.parquet", pio.ReadOption{})
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.PFile.Close()) }()

			root, err := NewSchemaTree(context.Background(), pr, tc.option)
			require.NoError(t, err)

			filtered := map[string]string{}
			for _, child := range root.Children {
				if child.BloomFilter != "" {
					filtered[child.Name] = child.BloomFilterSize
				}
			}
			if !tc.wantFilters {
				require.Empty(t, filtered)
				return
			}
			require.Equal(t, map[string]string{"ID": "1024", "Name": "4096", "Score": "1024"}, filtered)
		})
	}
}

func TestBuildBloomFilterMapCancelledContext(t *testing.T) {
	pr, err := pio.NewParquetFileReader(context.Background(), "../testdata/bloom-filter.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() {
		_ = pr.PFile.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = buildBloomFilterMap(ctx, pr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bloom filter size")
}
