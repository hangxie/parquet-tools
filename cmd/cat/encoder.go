package cat

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func (c Cmd) encoder(ctx context.Context, rowChan chan any, outputChan chan encodedRow, budget *outputBudget, schemaHandler *schema.SchemaHandler, fieldList []string, unknownCols map[string]struct{}) error {
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

	pool := make(chan *rowFormatter, formatterPoolSize)
	for range formatterPoolSize {
		f := newRowFormatter(c.Format)
		f.release = func() {
			f.recycle(budget)
			budget.release(f.weight)
			pool <- f
		}
		pool <- f
	}
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

			// The printer owns a formatter's buffer until it releases it.
			var formatter *rowFormatter
			select {
			case <-ctx.Done():
				return ctx.Err()
			case formatter = <-pool:
			}
			budget.reclaim(formatter.buffer.Cap())
			if err := formatter.encode(rowStruct, fieldList); err != nil {
				return err
			}
			if formatter.weight, err = budget.acquire(ctx, formatter.buffer.Cap()); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case outputChan <- encodedRow{buffer: &formatter.buffer, release: formatter.release}:
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

// formatterPoolSize lets an encoder keep working while the printer writes its earlier rows.
const formatterPoolSize = 16

type rowFormatter struct {
	format  string
	buffer  bytes.Buffer
	json    *json.Encoder
	csv     *csv.Writer
	release func()
	weight  int64
}

func newRowFormatter(format string) *rowFormatter {
	f := &rowFormatter{format: format}
	f.json = json.NewEncoder(&f.buffer)
	f.csv = csv.NewWriter(&f.buffer)
	f.csv.Comma = delimiter[format].fieldDelimiter
	return f
}

func (f *rowFormatter) encode(row any, fieldList []string) error {
	f.buffer.Reset()
	switch f.format {
	case "json", "jsonl":
		if err := f.json.Encode(row); err != nil {
			return err
		}
		f.buffer.Truncate(f.buffer.Len() - 1)
	case "csv", "tsv":
		if err := f.encodeRecord(mapToStrList(row.(map[string]any), fieldList)); err != nil {
			return err
		}
		f.buffer.Truncate(len(bytes.TrimRight(f.buffer.Bytes(), "\n")))
	default:
		return fmt.Errorf("unsupported format: [%s]", f.format)
	}
	return nil
}

// encodeRecord writes one CSV/TSV record, keeping its trailing newline.
func (f *rowFormatter) encodeRecord(values []string) error {
	f.buffer.Reset()
	if err := f.csv.Write(values); err != nil {
		return err
	}
	f.csv.Flush()
	return f.csv.Error()
}

// recycle keeps the buffer for reuse only while the budget's retained capacity allows.
func (f *rowFormatter) recycle(budget *outputBudget) {
	if !budget.retain(f.buffer.Cap()) {
		f.buffer = bytes.Buffer{}
	}
}
