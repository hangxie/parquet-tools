package cat

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func (c Cmd) encoder(ctx context.Context, rowChan chan any, outputChan chan string, schemaHandler *schema.SchemaHandler, fieldList []string, unknownCols map[string]struct{}) error {
	var geoMode types.GeospatialJSONMode
	switch c.GeoFormat {
	case "hex":
		geoMode = types.GeospatialModeHex
	case "base64":
		geoMode = types.GeospatialModeBase64
	case "hybrid":
		geoMode = types.GeospatialModeHybrid
	default:
		geoMode = types.GeospatialModeGeoJSON
	}
	geoOpt := marshal.WithGeospatialConfig(types.NewGeospatialConfig(
		types.WithGeometryJSONMode(geoMode),
		types.WithGeographyJSONMode(geoMode),
	))

	strBuilder := new(strings.Builder)
	csvWriter := csv.NewWriter(strBuilder)
	csvWriter.Comma = delimiter[c.Format].fieldDelimiter
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, more := <-rowChan:
			if !more {
				return nil
			}
			rowStruct, err := c.convertRow(row, schemaHandler, geoOpt)
			if err != nil {
				return err
			}
			if !c.RawUnknown && c.ValueMode != "raw" {
				nullifyUnknownCols(rowStruct, unknownCols)
			}

			// Format the row as a string based on the format
			var formattedRow string
			switch c.Format {
			case "json", "jsonl":
				buf, err := json.Marshal(rowStruct)
				if err != nil {
					return err
				}
				formattedRow = string(buf)
			case "csv", "tsv":
				values := mapToStrList(rowStruct.(map[string]any), fieldList)
				line, err := c.valuesToCSV(values, strBuilder, csvWriter)
				if err != nil {
					return err
				}
				formattedRow = strings.TrimRight(line, "\n")
			default:
				return fmt.Errorf("unsupported format: [%s]", c.Format)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case outputChan <- formattedRow:
			}
		}
	}
}

func (c Cmd) convertRow(row any, handler *schema.SchemaHandler, geoOpt marshal.JSONConvertOption) (any, error) {
	if c.ValueMode == "raw" {
		return rawValue(reflect.ValueOf(row), handler.GetRootInName(), handler)
	}
	return marshal.ConvertToJSONFriendly(row, handler, geoOpt)
}
