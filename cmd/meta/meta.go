package meta

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// Cmd is a kong command for meta
type Cmd struct {
	FailOnInt96     bool   `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
	ShowKeyMetadata bool   `help:"show key_metadata fields only (no decryption keys needed); useful for identifying KMS key IDs." name:"show-key-metadata" default:"false"`
	URI             string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
}

// Run does actual meta job
func (c Cmd) Run(ctx context.Context) error {
	if c.ShowKeyMetadata {
		hints, err := pio.ReadEncryptionKeyHints(ctx, c.URI, c.ReadOption)
		if err != nil {
			return err
		}
		if hints == nil {
			return fmt.Errorf("file is not encrypted")
		}
		buf, err := json.Marshal(hints)
		if err != nil {
			return err
		}
		fmt.Println(string(buf))
		return nil
	}

	reader, err := pio.NewParquetFileReader(ctx, c.URI, c.ReadOption)
	if err != nil {
		return err
	}

	schemaRoot, err := pschema.NewSchemaTree(ctx, reader, pschema.SchemaOption{FailOnInt96: c.FailOnInt96, SkipPageEncoding: true})
	if err != nil {
		return err
	}

	inExNameMap := schemaRoot.GetInExNameMap()
	pathMap := schemaRoot.GetPathMap()
	bloomSizeMap := pschema.BloomFilterSizeMap(reader)

	rowGroups, err := c.buildRowGroups(reader.Footer.RowGroups, inExNameMap, pathMap, bloomSizeMap)
	if err != nil {
		return err
	}

	meta := parquetMeta{
		Version:             reader.Footer.Version,
		NumRows:             reader.Footer.NumRows,
		NumRowGroups:        len(rowGroups),
		CreatedBy:           reader.Footer.CreatedBy,
		KeyValueMetadata:    keyValueMetadata(reader.Footer.KeyValueMetadata),
		ColumnOrders:        columnOrderNames(reader.Footer.ColumnOrders),
		EncryptionAlgorithm: encryptionAlgorithmName(reader.Footer.EncryptionAlgorithm),
		RowGroups:           rowGroups,
	}
	if fc := reader.FileCrypto; fc != nil {
		if km := fc.GetKeyMetadata(); len(km) > 0 {
			encoded := base64.StdEncoding.EncodeToString(km)
			meta.FooterKeyMetadata = &encoded
		}
	} else if reader.Footer != nil {
		if km := reader.Footer.GetFooterSigningKeyMetadata(); len(km) > 0 {
			encoded := base64.StdEncoding.EncodeToString(km)
			meta.FooterKeyMetadata = &encoded
		}
	}
	buf, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	fmt.Println(string(buf))

	return nil
}

func (c Cmd) buildRowGroups(rowGroups []*parquet.RowGroup, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) ([]rowGroupMeta, error) {
	result := make([]rowGroupMeta, len(rowGroups))
	for i, rg := range rowGroups {
		sortingColumns := sortingColumnMeta(rg.SortingColumns)
		columns, err := c.buildColumns(rg, sortingColumns, inExNameMap, pathMap, bloomSizeMap)
		if err != nil {
			return nil, err
		}
		result[i] = rowGroupMeta{
			NumRows:             rg.NumRows,
			TotalByteSize:       rg.TotalByteSize,
			FileOffset:          rg.FileOffset,
			TotalCompressedSize: rg.TotalCompressedSize,
			Ordinal:             rg.Ordinal,
			SortingColumns:      sortingColumns,
			Columns:             columns,
		}
	}
	return result, nil
}

func (c Cmd) buildColumns(rg *parquet.RowGroup, sortingColumns []sortingMeta, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) ([]columnMeta, error) {
	columns := make([]columnMeta, len(rg.Columns))
	for i, col := range rg.Columns {
		column, err := c.buildColumnMeta(col, sortingColumns, i, inExNameMap, pathMap, bloomSizeMap)
		if err != nil {
			return nil, err
		}
		columns[i] = column
	}
	return columns, nil
}

func (c Cmd) buildColumnMeta(col *parquet.ColumnChunk, sortingColumns []sortingMeta, colIndex int, inExNameMap map[string][]string, pathMap map[string]*pschema.SchemaNode, bloomSizeMap map[string]int32) (columnMeta, error) {
	column := columnMeta{
		PathInSchema:         col.MetaData.PathInSchema,
		Type:                 col.MetaData.Type.String(),
		ConvertedType:        nil,
		LogicalType:          nil,
		Encodings:            pschema.EncodingToString(col.MetaData.Encodings),
		CompressedSize:       col.MetaData.TotalCompressedSize,
		UncompressedSize:     col.MetaData.TotalUncompressedSize,
		NumValues:            col.MetaData.NumValues,
		FilePath:             col.FilePath,
		FileOffset:           col.FileOffset,
		DataPageOffset:       col.MetaData.DataPageOffset,
		IndexPageOffset:      col.MetaData.IndexPageOffset,
		DictionaryPageOffset: col.MetaData.DictionaryPageOffset,
		OffsetIndexOffset:    col.OffsetIndexOffset,
		OffsetIndexLength:    col.OffsetIndexLength,
		ColumnIndexOffset:    col.ColumnIndexOffset,
		ColumnIndexLength:    col.ColumnIndexLength,
		KeyValueMetadata:     keyValueMetadata(col.MetaData.KeyValueMetadata),
		EncodingStats:        encodingStatistics(col.MetaData.EncodingStats),
		SizeStatistics:       sizeStatistics(col.MetaData.SizeStatistics),
		GeospatialStatistics: geospatialStatistics(col.MetaData.GeospatialStatistics),
		MaxValue:             nil,
		MinValue:             nil,
		NullCount:            nil,
		DistinctCount:        nil,
		Index:                sortingToString(sortingColumns, colIndex),
		BloomFilterOffset:    col.MetaData.BloomFilterOffset,
		CompressionCodec:     col.MetaData.Codec.String(),
	}

	pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)

	// Use the correct bitset-only size from the bloom filter size map
	if size, ok := bloomSizeMap[pathKey]; ok && size > 0 {
		column.BloomFilterLength = &size
	}
	column.BloomFilterStorageLength = col.MetaData.BloomFilterLength

	if exPath, found := inExNameMap[pathKey]; found {
		column.PathInSchema = exPath
	}

	schemaNode := pathMap[pathKey]
	if schemaNode == nil {
		return columnMeta{}, fmt.Errorf("schema node not found for column path: [%s]", pathKey)
	}

	c.addTypeInformation(&column, schemaNode)

	if col.MetaData.Statistics != nil {
		c.addStatistics(&column, col.MetaData.Statistics, schemaNode)
	}

	// use bounding box for geospatial data if geospatial statistics exists
	if schemaNode.LogicalType != nil &&
		(schemaNode.LogicalType.IsSetGEOMETRY() || schemaNode.LogicalType.IsSetGEOGRAPHY()) &&
		col.MetaData.GeospatialStatistics != nil && col.MetaData.GeospatialStatistics.Bbox != nil {
		column.MinValue = []float64{
			col.MetaData.GeospatialStatistics.Bbox.Xmin,
			col.MetaData.GeospatialStatistics.Bbox.Ymin,
		}
		column.MaxValue = []float64{
			col.MetaData.GeospatialStatistics.Bbox.Xmax,
			col.MetaData.GeospatialStatistics.Bbox.Ymax,
		}
	}

	if cm := col.GetCryptoMetadata(); cm != nil {
		switch {
		case cm.ENCRYPTION_WITH_FOOTER_KEY != nil:
			mode := "FOOTER_KEY"
			column.EncryptionMode = &mode
		case cm.ENCRYPTION_WITH_COLUMN_KEY != nil:
			mode := "COLUMN_KEY"
			column.EncryptionMode = &mode
			if km := cm.ENCRYPTION_WITH_COLUMN_KEY.GetKeyMetadata(); len(km) > 0 {
				encoded := base64.StdEncoding.EncodeToString(km)
				column.KeyMetadata = &encoded
			}
		}
	}

	return column, nil
}

func (c Cmd) addTypeInformation(column *columnMeta, schemaNode *pschema.SchemaNode) {
	column.TypeLength = schemaNode.TypeLength
	if schemaNode.RepetitionType != nil {
		repetition := schemaNode.RepetitionType.String()
		column.RepetitionType = &repetition
	}
	column.Scale = schemaNode.Scale
	column.Precision = schemaNode.Precision
	column.FieldID = schemaNode.FieldID
	if ct := schemaNode.ConvertedTypeString(); ct != "" {
		column.ConvertedType = new(ct)
	}
	if lt := schemaNode.LogicalTypeString(); lt != "" {
		column.LogicalType = new(lt)
	}
}

func (c Cmd) addStatistics(column *columnMeta, statistics *parquet.Statistics, schemaNode *pschema.SchemaNode) {
	column.NullCount = statistics.NullCount
	column.DistinctCount = statistics.DistinctCount
	column.IsMinValueExact = statistics.IsMinValueExact
	column.IsMaxValueExact = statistics.IsMaxValueExact
	column.MinValue, column.MaxValue = schemaNode.DecodeStatistics(statistics)
	column.MinValue = normalizeNegativeZero(column.MinValue)
	column.MaxValue = normalizeNegativeZero(column.MaxValue)
}

// normalizeNegativeZero converts IEEE 754 negative zero floats to positive zero
// so that JSON output is consistent with standard JSON tools that treat -0 as 0.
func normalizeNegativeZero(v any) any {
	switch f := v.(type) {
	case float32:
		if math.Signbit(float64(f)) && f == 0 {
			return float32(0)
		}
	case float64:
		if math.Signbit(f) && f == 0 {
			return float64(0)
		}
	}
	return v
}

func sortingToString(sortingColumns []sortingMeta, columnIndex int) *string {
	for _, indexCol := range sortingColumns {
		if indexCol.ColumnIndex == int32(columnIndex) {
			return new(indexCol.Direction)
		}
	}
	return nil
}
