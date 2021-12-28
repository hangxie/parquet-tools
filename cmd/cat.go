package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/xitongsys/parquet-go/parquet"
)

// CatCmd is a kong command for cat
type CatCmd struct {
	CommonOption
	Skip        uint32  `short:"k" help:"Skip rows before apply other logics." default:"0"`
	Limit       uint64  `short:"l" help:"Max number of rows to output, 0 means no limit." default:"0"`
	PageSize    int     `short:"p" help:"Pagination size to read from Parquet." default:"1000"`
	SampleRatio float64 `short:"s" help:"Sample ratio (0.0-1.0)." default:"1.0"`
	Format      string  `help:"output format (json/jsonl)" enum:"json,jsonl" default:"json"`
}

// Run does actual cat job
func (c *CatCmd) Run(ctx *Context) error {
	if c.Limit == 0 {
		c.Limit = ^uint64(0)
	}
	if c.PageSize < 1 {
		return fmt.Errorf("invalid page size %d, needs to be at least 1", c.PageSize)
	}
	// note that sampling rate at 0.0 is allowed, while it does not output anything
	if c.SampleRatio < 0.0 || c.SampleRatio > 1.0 {
		return fmt.Errorf("invalid sampling %f, needs to be between 0.0 and 1.0", c.SampleRatio)
	}
	if c.Format != "json" && c.Format != "jsonl" {
		// should never reach here
		return fmt.Errorf("unknown format: %s", c.Format)
	}

	reader, err := newParquetFileReader(c.URI)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	// retrieve schema for better formatting
	schemaRoot := newSchemaTree(reader)
	decimalFields := getAllDecimalFields("", schemaRoot)

	delimiter := map[string]struct {
		begin string
		line  string
		end   string
	}{
		"json":  {"[", ",", "]"},
		"jsonl": {"", "\n", ""},
	}

	// deal with skip
	if c.Skip != 0 {
		// Do not abort if c.Skip is greater than total number of rows
		// This gives users flexibility to handle this scenario by themselves
		if err := reader.SkipRows(int64(c.Skip)); err != nil {
			return fmt.Errorf("failed to skip %d rows: %s", c.Skip, err)
		}
	}

	// Output rows one by one to avoid running out of memory with a jumbo list
	fmt.Print(delimiter[c.Format].begin)
	rand.Seed(time.Now().UnixNano())
	for counter := uint64(0); counter < c.Limit; {
		rows, err := reader.ReadByNumber(c.PageSize)
		if err != nil {
			return fmt.Errorf("failed to cat: %s", err)
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			if rand.Float64() >= c.SampleRatio {
				continue
			}
			if counter != 0 {
				fmt.Print(delimiter[c.Format].line)
			}

			// convert binary formation of DECIMAL to human readable string
			rowValue := reflect.ValueOf(&row).Elem()
			tmp := reflect.New(rowValue.Elem().Type()).Elem()
			tmp.Set(rowValue.Elem())
			for k, v := range decimalFields {
				if v.parquetType != parquet.Type_BYTE_ARRAY && v.parquetType != parquet.Type_FIXED_LEN_BYTE_ARRAY {
					continue
				}

				// TODO handle nested fields
				value := tmp.FieldByName(k)
				reformatStringDecimalValue(v, value)
			}
			rowValue.Set(tmp)

			// convert int or string based decimal to number
			buf, _ := json.Marshal(row)
			jsonString := string(buf)
			for k, v := range decimalFields {
				if v.parquetType == parquet.Type_BYTE_ARRAY || v.parquetType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
					value := gjson.Get(jsonString, k)
					if value.Type == gjson.Null {
						continue
					}
					decimalValue, err := strconv.ParseFloat(value.String(), 64)
					if err != nil {
						return err
					}
					jsonString, _ = sjson.Set(jsonString, k, decimalValue)
				} else if v.parquetType == parquet.Type_INT32 || v.parquetType == parquet.Type_INT64 {
					intValue := gjson.Get(jsonString, k).Int()
					decimalValue := float64(intValue) / math.Pow10(v.scale)
					jsonString, _ = sjson.Set(jsonString, k, decimalValue)
				}
			}

			fmt.Print(jsonString)

			counter += 1
			if counter >= c.Limit {
				break
			}
		}
	}
	fmt.Println(delimiter[c.Format].end)

	return nil
}
