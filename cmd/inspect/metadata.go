package inspect

import "github.com/hangxie/parquet-go/v3/parquet"

func addKeyValueMetadata(output map[string]any, values []*parquet.KeyValue) {
	if len(values) == 0 {
		return
	}
	metadata := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		entry := map[string]any{"key": value.Key}
		if value.Value != nil {
			entry["value"] = *value.Value
		}
		metadata = append(metadata, entry)
	}
	output["keyValueMetadata"] = metadata
}

func addRowGroupMetadata(output map[string]any, rowGroup *parquet.RowGroup) {
	if rowGroup.FileOffset != nil {
		output["fileOffset"] = *rowGroup.FileOffset
	}
	if rowGroup.TotalCompressedSize != nil {
		output["totalCompressedSize"] = *rowGroup.TotalCompressedSize
	}
	if rowGroup.Ordinal != nil {
		output["ordinal"] = *rowGroup.Ordinal
	}
	if len(rowGroup.SortingColumns) > 0 {
		columns := make([]map[string]any, 0, len(rowGroup.SortingColumns))
		for _, column := range rowGroup.SortingColumns {
			if column == nil {
				continue
			}
			direction := "ASC"
			if column.Descending {
				direction = "DESC"
			}
			columns = append(columns, map[string]any{
				"columnIndex": column.ColumnIdx,
				"direction":   direction,
				"nullsFirst":  column.NullsFirst,
			})
		}
		output["sortingColumns"] = columns
	}
}

func addColumnMetadata(output map[string]any, column *parquet.ColumnChunk) {
	metadata := column.MetaData
	output["fileOffset"] = column.FileOffset
	output["dataPageOffset"] = metadata.DataPageOffset
	addOptional(output, "filePath", column.FilePath)
	addOptional(output, "indexPageOffset", metadata.IndexPageOffset)
	addOptional(output, "dictionaryPageOffset", metadata.DictionaryPageOffset)
	addOptional(output, "offsetIndexOffset", column.OffsetIndexOffset)
	addOptional(output, "offsetIndexLength", column.OffsetIndexLength)
	addOptional(output, "columnIndexOffset", column.ColumnIndexOffset)
	addOptional(output, "columnIndexLength", column.ColumnIndexLength)
	addKeyValueMetadata(output, metadata.KeyValueMetadata)

	if len(metadata.EncodingStats) > 0 {
		values := make([]map[string]any, 0, len(metadata.EncodingStats))
		for _, statistic := range metadata.EncodingStats {
			if statistic != nil {
				values = append(values, map[string]any{"pageType": statistic.PageType.String(), "encoding": statistic.Encoding.String(), "count": statistic.Count})
			}
		}
		output["encodingStats"] = values
	}
	if statistics := metadata.SizeStatistics; statistics != nil {
		value := map[string]any{}
		addOptional(value, "unencodedByteArrayDataBytes", statistics.UnencodedByteArrayDataBytes)
		if statistics.RepetitionLevelHistogram != nil {
			value["repetitionLevelHistogram"] = statistics.RepetitionLevelHistogram
		}
		if statistics.DefinitionLevelHistogram != nil {
			value["definitionLevelHistogram"] = statistics.DefinitionLevelHistogram
		}
		output["sizeStatistics"] = value
	}
	if statistics := metadata.GeospatialStatistics; statistics != nil {
		value := map[string]any{}
		if statistics.GeospatialTypes != nil {
			value["geospatialTypes"] = statistics.GeospatialTypes
		}
		if box := statistics.Bbox; box != nil {
			bounds := map[string]any{"xMin": box.Xmin, "xMax": box.Xmax, "yMin": box.Ymin, "yMax": box.Ymax}
			addOptional(bounds, "zMin", box.Zmin)
			addOptional(bounds, "zMax", box.Zmax)
			addOptional(bounds, "mMin", box.Mmin)
			addOptional(bounds, "mMax", box.Mmax)
			value["boundingBox"] = bounds
		}
		output["geospatialStatistics"] = value
	}
}

func addSchemaElementMetadata(output map[string]any, element *parquet.SchemaElement) {
	if element == nil {
		return
	}
	addOptional(output, "typeLength", element.TypeLength)
	if element.RepetitionType != nil {
		output["repetitionType"] = element.RepetitionType.String()
	}
	addOptional(output, "scale", element.Scale)
	addOptional(output, "precision", element.Precision)
	addOptional(output, "fieldId", element.FieldID)
}

func addFileMetadata(output map[string]any, footer *parquet.FileMetaData) {
	addKeyValueMetadata(output, footer.KeyValueMetadata)
	if len(footer.ColumnOrders) > 0 {
		orders := make([]string, len(footer.ColumnOrders))
		for i, order := range footer.ColumnOrders {
			orders[i] = "UNDEFINED"
			if order != nil && order.TYPE_ORDER != nil {
				orders[i] = "TYPE_DEFINED_ORDER"
			}
		}
		output["columnOrders"] = orders
	}
	if algorithm := footer.EncryptionAlgorithm; algorithm != nil {
		switch {
		case algorithm.AES_GCM_V1 != nil:
			output["encryptionAlgorithm"] = "AES_GCM_V1"
		case algorithm.AES_GCM_CTR_V1 != nil:
			output["encryptionAlgorithm"] = "AES_GCM_CTR_V1"
		default:
			output["encryptionAlgorithm"] = "UNKNOWN"
		}
	}
}

func addOptional[T any](output map[string]any, name string, value *T) {
	if value != nil {
		output[name] = *value
	}
}
