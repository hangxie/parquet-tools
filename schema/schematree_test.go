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
	if option.FailOnInt96 || option.SkipPageEncoding {
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
	require.NotPanics(t, func() { require.Empty(t, buildBloomFilterMap(pr)) })
	require.NotPanics(t, func() { require.Empty(t, BloomFilterSizeMap(pr)) })
}

func TestBloomFilterMapsFallBackWhenEveryChunkIsEmpty(t *testing.T) {
	// With no row group holding values there is no chunk to prefer, so the
	// column's own metadata is read rather than the column being dropped.
	offset := int64(1024)
	// BloomFilterSize is promoted from an unexported embedded struct, so it
	// cannot be set in a composite literal.
	colInfo := &common.Tag{InName: "Col"}
	colInfo.BloomFilterSize = 64

	pr := &reader.ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
					PathInSchema:      []string{"col"},
					NumValues:         0,
					BloomFilterOffset: &offset,
				}}}},
			},
		},
		SchemaHandler: &pqschema.SchemaHandler{
			Infos:    []*common.Tag{{InName: "Root"}, colInfo},
			MapIndex: map[string]int32{common.PathToStr([]string{"Root", "col"}): 1},
		},
	}

	got := buildBloomFilterMap(pr)
	require.Equal(t, map[string]bloomFilterInfo{"col": {Enabled: true, Size: 64}}, got)

	require.Equal(t, map[string]int32{"col": 64}, BloomFilterSizeMap(pr))
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
	// No size: the library measures filters in row group 0, which is empty here,
	// so this is the state a real file in this shape would arrive in.
	require.Equal(t, map[string]bloomFilterInfo{"col": {Enabled: true}}, buildBloomFilterMap(pr))
}

func TestBloomFilterFoundPastTheFirstPopulatedChunk(t *testing.T) {
	// Values in row group 0 with no filter, a filter in row group 1. The first
	// populated chunk alone would report no filter for a file that has one.
	offset := int64(4096)
	chunk := func(numValues int64, bloom *int64) *parquet.ColumnChunk {
		return &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
			PathInSchema:      []string{"col"},
			NumValues:         numValues,
			BloomFilterOffset: bloom,
		}}
	}

	pr := &reader.ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{chunk(10, nil)}},
				// A row group short of this column must be stepped over, not indexed.
				{Columns: []*parquet.ColumnChunk{}},
				{Columns: []*parquet.ColumnChunk{chunk(10, &offset)}},
			},
		},
		SchemaHandler: &pqschema.SchemaHandler{
			Infos:    []*common.Tag{{InName: "Root"}, {InName: "Col"}},
			MapIndex: map[string]int32{common.PathToStr([]string{"Root", "col"}): 1},
		},
	}

	require.Equal(t, map[string]bloomFilterInfo{"col": {Enabled: true}}, buildBloomFilterMap(pr))
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

func TestBloomFilterSizeMapOmitsUnsizedFilters(t *testing.T) {
	// The library sizes filters in row group 0 only, so one found later has no
	// size. A size map leaves it out rather than claiming zero bytes.
	offset := int64(2048)
	pr := &reader.ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
					PathInSchema: []string{"col"}, NumValues: 0,
				}}}},
				{Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
					PathInSchema: []string{"col"}, NumValues: 9, BloomFilterOffset: &offset,
				}}}},
			},
		},
		SchemaHandler: &pqschema.SchemaHandler{
			Infos:    []*common.Tag{{InName: "Root"}, {InName: "Col"}},
			MapIndex: map[string]int32{common.PathToStr([]string{"Root", "col"}): 1},
		},
	}

	require.Equal(t, map[string]bloomFilterInfo{"col": {Enabled: true}}, buildBloomFilterMap(pr),
		"the filter is still reported, just without a size")
	require.Empty(t, BloomFilterSizeMap(pr), "a sizeless filter has no entry in a size map")

	// A column the schema handler cannot resolve has no size to look up either.
	pr.SchemaHandler.MapIndex = map[string]int32{}
	require.Empty(t, BloomFilterSizeMap(pr))
	require.Equal(t, map[string]bloomFilterInfo{"col": {Enabled: true}}, buildBloomFilterMap(pr))
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
