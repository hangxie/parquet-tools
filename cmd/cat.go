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
					encodeNestedBinaryString(tmp, strings.Split(k, "."), v)
				}
			}
			rowValue.Set(tmp)

			// convert to strict struct type to interface so we can change the value for formatting
			var iface interface{}
			if buf, err := json.Marshal(row); err != nil {
				return fmt.Errorf("failed to convert value (encoding): %s", err.Error())
			} else if err := json.Unmarshal(buf, &iface); err != nil {
				return fmt.Errorf("failed to convert value (decoding): %s", err.Error())
			}
			for k, v := range reinterpretFields {
				if err := reinterpretNestedFields(&iface, strings.Split(k, "."), v); err != nil {
					return fmt.Errorf("failed to reinterpret fields [%s]: %s", k, err.Error())
				}
			}
			buf, err := json.Marshal(iface)
			if err != nil {
				return fmt.Errorf("failed to output (encoding): %s", err.Error())
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
	if !value.IsValid() {
		return
	}

	if len(locator) == 0 {
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				return
			}
			value = value.Elem()
		}
		if !value.IsValid() {
			return
		}
		buf := []byte(value.String())
		if attr.convertedType == parquet.ConvertedType_INTERVAL {
			// INTERVAL uses LittleEndian, DECIMAL uses BigEndian
			// make sure all decimal-like value are all BigEndian
			for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
				buf[i], buf[j] = buf[j], buf[i]
			}
		}
		value.SetString(base64.StdEncoding.EncodeToString(buf))
		return
	}

	if value.Kind() != reflect.Map && value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
		value = value.FieldByName(locator[0])
		if value.Kind() == reflect.Ptr && !value.IsNil() {
			value = value.Elem()
		}
		if !value.IsValid() {
			return
		}
	}

	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		for elementIndex := 0; elementIndex < value.Len(); elementIndex++ {
			encodeNestedBinaryString(value.Index(elementIndex), locator[2:], attr)
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			if locator[1] == "Key" {
				v := value.MapIndex(key)
				newKey := reflect.New(key.Type()).Elem()
				newKey.Set(key)
				encodeNestedBinaryString(newKey, locator[2:], attr)
				value.SetMapIndex(newKey, v)
				value.SetMapIndex(key, reflect.Value{})
			} else if locator[1] == "Value" {
				v := value.MapIndex(key)
				newValue := reflect.New(v.Type()).Elem()
				newValue.Set(v)
				encodeNestedBinaryString(newValue, locator[2:], attr)
				value.SetMapIndex(key, newValue)
			}
		}
	default:
		encodeNestedBinaryString(value, locator[1:], attr)
	}
}

func reinterpretNestedFields(iface *interface{}, locator []string, attr ReinterpretField) error {
	if iface == nil || *iface == nil {
		return nil
	}
	v := reflect.ValueOf(*iface)
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		if len(locator) == 0 {
			return fmt.Errorf("cannot reinterpret a list")
		}
		for i := range (*iface).([]interface{}) {
			value := (*iface).([]interface{})[i]
			if err := reinterpretNestedFields(&value, locator[1:], attr); err != nil {
				return err
			}
			(*iface).([]interface{})[i] = value
		}
		return nil
	case reflect.Map:
		if len(locator) == 0 {
			return fmt.Errorf("cannot reinterpret a map")
		}
		mapValue := (*iface).(map[string]interface{})
		if locator[0] == "Key" {
			newMapValue := make(map[string]interface{})
			for k, v := range mapValue {
				var newKey interface{} = k
				if err := reinterpretNestedFields(&newKey, locator[1:], attr); err != nil {
					return err
				}
				// fmt.Println("DEBUG", k, newKey, attr.convertedType.String(), attr.parquetType.String())
				switch val := newKey.(type) {
				case string:
					newMapValue[val] = v
				case float64:
					format := fmt.Sprintf("%%0.%df", attr.scale)
					newMapValue[fmt.Sprintf(format, val)] = v
				default:
					return fmt.Errorf("reinterpret returned invalid type: %s", reflect.TypeOf(newKey))
				}
			}
			mapValue = newMapValue
		} else if locator[0] == "Value" {
			for k, v := range mapValue {
				if err := reinterpretNestedFields(&v, locator[1:], attr); err != nil {
					return err
				}
				mapValue[k] = v
			}
		} else {
			scalarValue := mapValue[locator[0]]
			if err := reinterpretNestedFields(&scalarValue, locator[1:], attr); err != nil {
				return err
			}
			mapValue[locator[0]] = scalarValue
		}
		*iface = mapValue
		return nil
	}

	// scalar type
	switch attr.parquetType {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if _, ok := (*iface).(string); !ok {
			return fmt.Errorf("INTERVAL/DECIMAL values need to be string type: %s", reflect.TypeOf(*iface))
		}
		encoded, err := base64.StdEncoding.DecodeString((*iface).(string))
		if err != nil {
			return fmt.Errorf("[%s] is not base64 encoded: %s", (*iface).(string), err.Error())
		}
		*iface, err = strconv.ParseFloat(types.DECIMAL_BYTE_ARRAY_ToString(encoded, attr.precision, attr.scale), 64)
		if err != nil {
			return err
		}
	case parquet.Type_INT32, parquet.Type_INT64:
		if v, ok := (*iface).(float64); ok {
			*iface = v / math.Pow10(attr.scale)
		} else if v, ok := (*iface).(string); ok {
			if float64Value, err := strconv.ParseFloat(v, 64); err != nil {
				return fmt.Errorf("failed to parse string [%s] to float64: %s", v, err.Error())
			} else {
				*iface = float64Value / math.Pow10(attr.scale)
			}
		} else {
			return fmt.Errorf("INT32/INT64 values need to be float64 type: %s", reflect.TypeOf(*iface))
		}
	case parquet.Type_INT96:
		if _, ok := (*iface).(string); !ok {
			return fmt.Errorf("INT96 values need to be string type: %s", reflect.TypeOf(*iface))
		}
		encoded, err := base64.StdEncoding.DecodeString((*iface).(string))
		if err != nil {
			return fmt.Errorf("[%s] is not base64 encoded: %s", strings.Join(locator, "."), err.Error())
		}
		*iface = types.INT96ToTime(string(encoded)).Format(time.RFC3339Nano)
	}

	return nil
}
