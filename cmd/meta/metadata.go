package meta

import "github.com/hangxie/parquet-go/v3/parquet"

type keyValueMeta struct {
	Key   string
	Value *string `json:",omitempty"`
}

type sortingMeta struct {
	ColumnIndex int32
	Direction   string
	NullsFirst  bool
}

type encodingStatMeta struct {
	PageType string
	Encoding string
	Count    int32
}

type sizeStatisticsMeta struct {
	UnencodedByteArrayDataBytes *int64  `json:",omitempty"`
	RepetitionLevelHistogram    []int64 `json:",omitempty"`
	DefinitionLevelHistogram    []int64 `json:",omitempty"`
}

type boundingBoxMeta struct {
	XMin float64
	XMax float64
	YMin float64
	YMax float64
	ZMin *float64 `json:",omitempty"`
	ZMax *float64 `json:",omitempty"`
	MMin *float64 `json:",omitempty"`
	MMax *float64 `json:",omitempty"`
}

type geospatialStatisticsMeta struct {
	BoundingBox     *boundingBoxMeta `json:",omitempty"`
	GeospatialTypes []int32          `json:",omitempty"`
}

type columnMeta struct {
	PathInSchema         []string
	Type                 string
	TypeLength           *int32  `json:",omitempty"`
	RepetitionType       *string `json:",omitempty"`
	ConvertedType        *string `json:",omitempty"`
	LogicalType          *string `json:",omitempty"`
	Scale                *int32  `json:",omitempty"`
	Precision            *int32  `json:",omitempty"`
	FieldID              *int32  `json:",omitempty"`
	Encodings            []string
	CompressedSize       int64
	UncompressedSize     int64
	NumValues            int64
	NullCount            *int64  `json:",omitempty"`
	DistinctCount        *int64  `json:",omitempty"`
	MaxValue             any     `json:",omitempty"`
	MinValue             any     `json:",omitempty"`
	IsMaxValueExact      *bool   `json:",omitempty"`
	IsMinValueExact      *bool   `json:",omitempty"`
	Index                *string `json:",omitempty"` // Legacy per-column projection of row-group SortingColumns.
	FilePath             *string `json:",omitempty"`
	FileOffset           int64
	DataPageOffset       int64
	IndexPageOffset      *int64                    `json:",omitempty"`
	DictionaryPageOffset *int64                    `json:",omitempty"`
	OffsetIndexOffset    *int64                    `json:",omitempty"`
	OffsetIndexLength    *int32                    `json:",omitempty"`
	ColumnIndexOffset    *int64                    `json:",omitempty"`
	ColumnIndexLength    *int32                    `json:",omitempty"`
	KeyValueMetadata     []keyValueMeta            `json:",omitempty"`
	EncodingStats        []encodingStatMeta        `json:",omitempty"`
	SizeStatistics       *sizeStatisticsMeta       `json:",omitempty"`
	GeospatialStatistics *geospatialStatisticsMeta `json:",omitempty"`
	BloomFilterOffset    *int64                    `json:",omitempty"`
	BloomFilterLength    *int32                    `json:",omitempty"`
	CompressionCodec     string
	EncryptionMode       *string `json:",omitempty"`
	KeyMetadata          *string `json:",omitempty"`
}

type rowGroupMeta struct {
	NumRows             int64
	TotalByteSize       int64
	FileOffset          *int64        `json:",omitempty"`
	TotalCompressedSize *int64        `json:",omitempty"`
	Ordinal             *int16        `json:",omitempty"`
	SortingColumns      []sortingMeta `json:",omitempty"`
	Columns             []columnMeta
}

type parquetMeta struct {
	Version             int32
	NumRows             int64
	NumRowGroups        int
	CreatedBy           *string        `json:",omitempty"`
	KeyValueMetadata    []keyValueMeta `json:",omitempty"`
	ColumnOrders        []string       `json:",omitempty"`
	EncryptionAlgorithm *string        `json:",omitempty"`
	FooterKeyMetadata   *string        `json:",omitempty"`
	RowGroups           []rowGroupMeta
}

func keyValueMetadata(values []*parquet.KeyValue) []keyValueMeta {
	result := make([]keyValueMeta, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, keyValueMeta{Key: value.Key, Value: value.Value})
		}
	}
	return result
}

func sortingColumnMeta(columns []*parquet.SortingColumn) []sortingMeta {
	result := make([]sortingMeta, 0, len(columns))
	for _, column := range columns {
		if column == nil {
			continue
		}
		direction := "ASC"
		if column.Descending {
			direction = "DESC"
		}
		result = append(result, sortingMeta{ColumnIndex: column.ColumnIdx, Direction: direction, NullsFirst: column.NullsFirst})
	}
	return result
}

func encryptionAlgorithmName(algorithm *parquet.EncryptionAlgorithm) *string {
	if algorithm == nil {
		return nil
	}
	if algorithm.AES_GCM_V1 != nil {
		return new("AES_GCM_V1")
	}
	if algorithm.AES_GCM_CTR_V1 != nil {
		return new("AES_GCM_CTR_V1")
	}
	return new("UNKNOWN")
}

func columnOrderNames(orders []*parquet.ColumnOrder) []string {
	result := make([]string, 0, len(orders))
	for _, order := range orders {
		if order != nil && order.TYPE_ORDER != nil {
			result = append(result, "TYPE_DEFINED_ORDER")
		} else {
			result = append(result, "UNDEFINED")
		}
	}
	return result
}

func encodingStatistics(statistics []*parquet.PageEncodingStats) []encodingStatMeta {
	result := make([]encodingStatMeta, 0, len(statistics))
	for _, statistic := range statistics {
		if statistic != nil {
			result = append(result, encodingStatMeta{PageType: statistic.PageType.String(), Encoding: statistic.Encoding.String(), Count: statistic.Count})
		}
	}
	return result
}

func sizeStatistics(statistics *parquet.SizeStatistics) *sizeStatisticsMeta {
	if statistics == nil {
		return nil
	}
	return &sizeStatisticsMeta{
		UnencodedByteArrayDataBytes: statistics.UnencodedByteArrayDataBytes,
		RepetitionLevelHistogram:    statistics.RepetitionLevelHistogram,
		DefinitionLevelHistogram:    statistics.DefinitionLevelHistogram,
	}
}

func geospatialStatistics(statistics *parquet.GeospatialStatistics) *geospatialStatisticsMeta {
	if statistics == nil {
		return nil
	}
	result := &geospatialStatisticsMeta{GeospatialTypes: statistics.GeospatialTypes}
	if box := statistics.Bbox; box != nil {
		result.BoundingBox = &boundingBoxMeta{
			XMin: box.Xmin, XMax: box.Xmax, YMin: box.Ymin, YMax: box.Ymax,
			ZMin: box.Zmin, ZMax: box.Zmax, MMin: box.Mmin, MMax: box.Mmax,
		}
	}
	return result
}
