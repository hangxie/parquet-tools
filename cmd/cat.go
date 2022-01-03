package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
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

var delimiter = map[string]struct {
	begin string
	line  string
	end   string
}{
	"json":  {"[", ",", "]"},
	"jsonl": {"", "\n", ""},
}

// Run does actual cat job
func (c *CatCmd) Run(ctx *Context) error {
	if c.PageSize < 1 {
		return fmt.Errorf("invalid page size %d, needs to be at least 1", c.PageSize)
	}
	if c.Limit == 0 {
		c.Limit = ^uint64(0)
	}
	// note that sampling rate at 0.0 is allowed, while it does not output anything
	if c.SampleRatio < 0.0 || c.SampleRatio > 1.0 {
		return fmt.Errorf("invalid sampling %f, needs to be between 0.0 and 1.0", c.SampleRatio)
	}
	if _, ok := delimiter[c.Format]; !ok {
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
	reinterpretFields := getReinterpretFields("", schemaRoot, true)

	// this is hack for https://github.com/xitongsys/parquet-go/issues/438
	if reader.GetNumRows() == 0 {
		c.Limit = 0
	}

	// Do not abort if c.Skip is greater than total number of rows
	// This gives users flexibility to handle this scenario by themselves
	if err := reader.SkipRows(int64(c.Skip)); err != nil {
		return fmt.Errorf("failed to skip %d rows: %s", c.Skip, err)
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

			// convert binary DECIMAL to human readable string
			rowValue := reflect.ValueOf(&row).Elem()
			tmp := reflect.New(rowValue.Elem().Type()).Elem()
			tmp.Set(rowValue.Elem())
			for k, v := range reinterpretFields {
				if v.parquetType == parquet.Type_BYTE_ARRAY || v.parquetType == parquet.Type_FIXED_LEN_BYTE_ARRAY || v.parquetType == parquet.Type_INT96 {
					reformatNestedString(tmp, strings.Split(k, "."), v)
				}
			}
			rowValue.Set(tmp)

			// convert int or string based decimal to number
			buf, _ := json.Marshal(row)
			jsonString := string(buf)
			for k, v := range reinterpretFields {
				value := gjson.Get(jsonString, k)
				if value.Type == gjson.Null {
					continue
				}
				switch v.parquetType {
				case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
					decimalValue, err := strconv.ParseFloat(value.String(), 64)
					if err != nil {
						return err
					}
					jsonString, _ = sjson.Set(jsonString, k, decimalValue)
				case parquet.Type_INT32, parquet.Type_INT64:
					jsonString, _ = sjson.Set(jsonString, k, float64(value.Int())/math.Pow10(v.scale))
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

func reformatNestedString(value reflect.Value, locator []string, attr ReinterpretField) {
	if len(locator) == 0 {
		reformatStringValue(attr, value)
		return
	}

	v := value.FieldByName(locator[0])
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		for elementIndex := 0; elementIndex < v.Len(); elementIndex++ {
			reformatNestedString(v.Index(elementIndex), locator[2:], attr)
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			if locator[1] == "Key" {
				key := iter.Key()
				value := iter.Value()

				// delete old key
				v.SetMapIndex(key, reflect.Value{})

				newKey := reflect.New(key.Type()).Elem()
				newKey.Set(key)
				reformatNestedString(newKey, locator[2:], attr)
				v.SetMapIndex(newKey, value)
			} else {
				newValue := reflect.New(iter.Value().Type()).Elem()
				newValue.Set(iter.Value())
				reformatNestedString(newValue, locator[2:], attr)
				v.SetMapIndex(iter.Key(), newValue)
			}
		}
	default:
		reformatNestedString(v, locator[1:], attr)
	}
}

func reformatStringValue(fieldAttr ReinterpretField, value reflect.Value) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}

	if !value.IsValid() {
		return
	}

	switch fieldAttr.parquetType {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		buf := stringToBytes(fieldAttr, value.String())
		newValue := types.DECIMAL_BYTE_ARRAY_ToString(buf, fieldAttr.precision, fieldAttr.scale)
		value.SetString(newValue)
	case parquet.Type_INT96:
		buf := value.String()
		newValue := types.INT96ToTime(buf).Format(time.RFC3339Nano)
		value.SetString(newValue)
	}
}
