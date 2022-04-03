package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

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

	reader, err := newParquetFileReader(c.CommonOption)
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

			rowValue := reflect.ValueOf(&row).Elem()
			tmp := reflect.New(rowValue.Elem().Type()).Elem()
			tmp.Set(rowValue.Elem())
			for k, v := range reinterpretFields {
				if v.parquetType == parquet.Type_BYTE_ARRAY || v.parquetType == parquet.Type_FIXED_LEN_BYTE_ARRAY || v.parquetType == parquet.Type_INT96 {
					encodeNestedBinaryString(tmp, strings.Split(k, ".")[1:], v)
				}
			}
			rowValue.Set(tmp)

			// convert to struct type to map of interface so we can change the value for formatting,
			// fail back to original data for any kind of errors
			var iface interface{}
			buf, err := json.Marshal(row)
			if err == nil {
				if err := json.Unmarshal(buf, &iface); err == nil {
					for k, v := range reinterpretFields {
						reinterpretNestedFields(&iface, strings.Split(k, ".")[1:], v)
					}
					if newBuf, err := json.Marshal(iface); err == nil {
						buf = newBuf
					}
				}
			}

			fmt.Print(string(buf))
			counter += 1
			if counter >= c.Limit {
				break
			}
		}
	}
	fmt.Println(delimiter[c.Format].end)

	return nil
}

func encodeNestedBinaryString(value reflect.Value, locator []string, attr ReinterpretField) {
	// dereference pointer
	if value.Kind() == reflect.Ptr {
		if !value.IsNil() {
			encodeNestedBinaryString(value.Elem(), locator, attr)
		}
		return
	}

	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		for elementIndex := 0; elementIndex < value.Len(); elementIndex++ {
			encodeNestedBinaryString(value.Index(elementIndex), locator[1:], attr)
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			switch locator[0] {
			case "Key":
				v := value.MapIndex(key)
				newKey := reflect.New(key.Type()).Elem()
				newKey.Set(key)
				encodeNestedBinaryString(newKey, locator[1:], attr)
				value.SetMapIndex(newKey, v)
				value.SetMapIndex(key, reflect.Value{})
			case "Value":
				v := value.MapIndex(key)
				newValue := reflect.New(v.Type()).Elem()
				newValue.Set(v)
				encodeNestedBinaryString(newValue, locator[1:], attr)
				value.SetMapIndex(key, newValue)
			}
		}
	case reflect.Struct:
		encodeNestedBinaryString(value.FieldByName(locator[0]), locator[1:], attr)
	case reflect.String:
		buf := stringToBytes(attr, value.String())
		value.SetString(base64.StdEncoding.EncodeToString(buf))
	}
}

func reinterpretNestedFields(iface *interface{}, locator []string, attr ReinterpretField) {
	if iface == nil || *iface == nil {
		return
	}
	v := reflect.ValueOf(*iface)
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		if len(locator) != 0 {
			for i := range (*iface).([]interface{}) {
				value := (*iface).([]interface{})[i]
				reinterpretNestedFields(&value, locator[1:], attr)
				(*iface).([]interface{})[i] = value
			}
		}
		return
	case reflect.Map:
		if len(locator) != 0 {
			mapValue := (*iface).(map[string]interface{})
			switch locator[0] {
			case "Key":
				newMapValue := make(map[string]interface{})
				for k, v := range mapValue {
					var newKey interface{} = k
					reinterpretNestedFields(&newKey, locator[1:], attr)

					// INT32/INT64 will be reinterpreted to float, while string DECIMAL and
					// INTERVAL type will be reinterpreted to string
					switch val := newKey.(type) {
					case string:
						newMapValue[val] = v
					case float64:
						format := fmt.Sprintf("%%0.%df", attr.scale)
						newMapValue[fmt.Sprintf(format, val)] = v
					}
				}
				mapValue = newMapValue
			case "Value":
				for k, v := range mapValue {
					reinterpretNestedFields(&v, locator[1:], attr)
					mapValue[k] = v
				}
			default:
				// this is a map serialized from struct, so keep dig into sub-fields
				scalarValue := mapValue[locator[0]]
				reinterpretNestedFields(&scalarValue, locator[1:], attr)
				mapValue[locator[0]] = scalarValue
			}
			*iface = mapValue
		}
		return
	}

	switch attr.parquetType {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		switch v := (*iface).(type) {
		case string:
			if encoded, err := base64.StdEncoding.DecodeString(v); err == nil {
				if f64, err := strconv.ParseFloat(types.DECIMAL_BYTE_ARRAY_ToString(encoded, attr.precision, attr.scale), 64); err == nil {
					*iface = f64
				}
			}
		}
	case parquet.Type_INT32, parquet.Type_INT64:
		switch v := (*iface).(type) {
		case float64:
			*iface = v / math.Pow10(attr.scale)
		case string:
			if f64, err := strconv.ParseFloat(v, 64); err == nil {
				*iface = f64 / math.Pow10(attr.scale)
			}
		}
	case parquet.Type_INT96:
		if _, ok := (*iface).(string); ok {
			if encoded, err := base64.StdEncoding.DecodeString((*iface).(string)); err == nil {
				*iface = types.INT96ToTime(string(encoded)).Format(time.RFC3339Nano)
			}
		}
	}
}
