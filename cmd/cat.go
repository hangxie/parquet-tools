package cmd

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/types"

	"github.com/hangxie/parquet-tools/internal"
)

// CatCmd is a kong command for cat
type CatCmd struct {
	internal.ReadOption
	Skip        uint32  `short:"k" help:"Skip rows before apply other logics." default:"0"`
	Limit       uint64  `short:"l" help:"Max number of rows to output, 0 means no limit." default:"0"`
	PageSize    int     `short:"p" help:"Pagination size to read from Parquet." default:"1000"`
	SampleRatio float32 `short:"s" help:"Sample ratio (0.0-1.0)." default:"1.0"`
	Format      string  `short:"f" help:"output format (json/jsonl/csv/tsv)" enum:"json,jsonl,csv,tsv" default:"json"`
	NoHeader    bool    `help:"(CSV/TSV only) do not output field name as header" default:"false"`
}

var delimiter = map[string]struct {
	begin          string
	lineDelimiter  string
	fieldDelimiter rune
	end            string
}{
	"json":  {"[", ",", ' ', "]"},
	"jsonl": {"", "\n", ' ', ""},
	"csv":   {"", "\n", ',', ""},
	"tsv":   {"", "\n", '\t', ""},
}

// here are performan number for different settings:
// - using https://dpla-provider-export.s3.amazonaws.com/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet
// - amateure test - on Mac with time command and Activity Monitor, numbers are for reference only
// page_size max_memory_usage time_taken
// 1K        1.9G             25s
// 10K       1.8G             15s
// 100K      2.4G             12s
// 1M        7.1G             15s
// 10M       52.1G            1m14s
const pageSize int64 = 100_000

// Run does actual cat job
func (c CatCmd) Run() error {
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

	fileReader, err := internal.NewParquetFileReader(c.ReadOption)
	if err != nil {
		return err
	}
	defer fileReader.PFile.Close()

	return c.outputRows(fileReader)
}

func (c CatCmd) outputHeader(fileReader *reader.ParquetReader, schemaRoot *internal.SchemaNode) ([]string, error) {
	if c.Format != "csv" && c.Format != "tsv" {
		// only CSV and TSV need header
		return nil, nil
	}

	fieldList := make([]string, len(schemaRoot.Children))
	for index, child := range schemaRoot.Children {
		if len(child.Children) != 0 {
			return nil, fmt.Errorf("field [%s] is not scalar type, cannot output in %s format", child.Name, c.Format)
		}
		fieldList[index] = child.Name
	}
	line, err := valuesToCSV(fieldList, delimiter[c.Format].fieldDelimiter)
	if err != nil {
		return nil, err
	}

	if !c.NoHeader {
		fmt.Print(line)
	}
	return fieldList, nil
}

func (c CatCmd) skipRows(fileReader *reader.ParquetReader) error {
	// Do not abort if c.Skip is greater than total number of rows
	// This gives users flexibility to handle this scenario by themselves

	// use pagination to avoid excessive memory usage, see https://github.com/xitongsys/parquet-go/issues/545
	rowsToSkip := int64(c.Skip)
	for ; rowsToSkip > pageSize; rowsToSkip -= pageSize {
		if err := fileReader.SkipRows(pageSize); err != nil {
			return fmt.Errorf("failed to skip %d rows: %s", c.Skip, err)
		}
	}
	if err := fileReader.SkipRows(rowsToSkip); err != nil {
		return fmt.Errorf("failed to skip %d rows: %s", c.Skip, err)
	}
	return nil
}

func (c CatCmd) outputRows(fileReader *reader.ParquetReader) error {
	schemaRoot := internal.NewSchemaTree(fileReader)

	// CSV snd TSV does not support nested schema
	fieldList, err := c.outputHeader(fileReader, schemaRoot)
	if err != nil {
		return err
	}

	// retrieve schema for better formatting
	reinterpretFields := schemaRoot.GetReinterpretFields("", true)

	// skip rows
	if err != c.skipRows(fileReader) {
		return err
	}

	// Output rows one by one to avoid running out of memory with a jumbo list
	fmt.Print(delimiter[c.Format].begin)
	for counter := uint64(0); counter < c.Limit; {
		rows, err := fileReader.ReadByNumber(c.PageSize)
		if err != nil {
			return fmt.Errorf("failed to cat: %s", err)
		}
		if len(rows) == 0 {
			break
		}

		for i := 0; i < len(rows) && counter < c.Limit; i++ {
			if rand.Float32() >= c.SampleRatio {
				continue
			}
			if counter != 0 {
				fmt.Print(delimiter[c.Format].lineDelimiter)
			}
			// there is no known error at this moment
			rowStruct, _ := rowToStruct(rows[i], reinterpretFields)
			switch c.Format {
			case "json", "jsonl":
				buf, _ := json.Marshal(rowStruct)
				fmt.Print(string(buf))
			case "csv", "tsv":
				flatValues := rowStruct.(map[string]interface{})
				values := make([]string, len(flatValues))
				for index, field := range fieldList {
					switch val := flatValues[field].(type) {
					case nil:
						// nil is just empty
					default:
						values[index] = fmt.Sprint(val)
					}
				}

				line, err := valuesToCSV(values, delimiter[c.Format].fieldDelimiter)
				if err != nil {
					return err
				}
				fmt.Print(strings.TrimRight(line, "\n"))
			}
			counter++
		}
	}
	fmt.Println(delimiter[c.Format].end)
	return nil
}

func valuesToCSV(values []string, delimiter rune) (string, error) {
	// there is no standard for CSV, use go's CSV module to maintain minimum compatibility
	buf := new(strings.Builder)
	csvWriter := csv.NewWriter(buf)
	csvWriter.Comma = delimiter
	if err := csvWriter.Write(values); err != nil {
		// this should never happen
		return "", err
	}
	csvWriter.Flush()
	return buf.String(), nil
}

func rowToStruct(row interface{}, reinterpretFields map[string]internal.ReinterpretField) (interface{}, error) {
	rowValue := reflect.ValueOf(&row).Elem()
	tmp := reflect.New(rowValue.Elem().Type()).Elem()
	tmp.Set(rowValue.Elem())
	for k, v := range reinterpretFields {
		// There are data types that are represented as string but they are actually not UTF8, they
		// need to be re-interpreted so we will base64 encode them here to avoid losing data. For
		// more details: https://github.com/xitongsys/parquet-go/issues/434
		if v.ParquetType == parquet.Type_BYTE_ARRAY || v.ParquetType == parquet.Type_FIXED_LEN_BYTE_ARRAY || v.ParquetType == parquet.Type_INT96 {
			encodeNestedBinaryString(tmp, strings.Split(k, ".")[1:], v)
		}
	}
	rowValue.Set(tmp)

	// convert to struct type to map of interface so we can change the value for formatting,
	// fail back to original data for any kind of errors
	buf, err := json.Marshal(row)
	if err != nil {
		return "", err
	}

	// this should not fail as we just Marshal it
	var iface interface{}
	_ = json.Unmarshal(buf, &iface)
	for k, v := range reinterpretFields {
		reinterpretNestedFields(&iface, strings.Split(k, ".")[1:], v)
	}
	return iface, nil
}

func encodeNestedBinaryString(value reflect.Value, locator []string, attr internal.ReinterpretField) {
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
		buf := internal.StringToBytes(attr, value.String())
		value.SetString(base64.StdEncoding.EncodeToString(buf))
	}
}

func reinterpretNestedFields(iface *interface{}, locator []string, attr internal.ReinterpretField) {
	if iface == nil || *iface == nil {
		return
	}
	v := reflect.ValueOf(*iface)
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		if len(locator) == 0 {
			return
		}
		for i := range (*iface).([]interface{}) {
			value := (*iface).([]interface{})[i]
			reinterpretNestedFields(&value, locator[1:], attr)
			(*iface).([]interface{})[i] = value
		}
	case reflect.Map:
		if len(locator) == 0 {
			return
		}
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
					format := fmt.Sprintf("%%0.%df", attr.Scale)
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
	default:
		reinterpretScalar(iface, locator, attr)
	}
}

func reinterpretScalar(iface *interface{}, locator []string, attr internal.ReinterpretField) {
	switch attr.ParquetType {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		switch v := (*iface).(type) {
		case string:
			if encoded, err := base64.StdEncoding.DecodeString(v); err == nil {
				if f64, err := strconv.ParseFloat(types.DECIMAL_BYTE_ARRAY_ToString(encoded, attr.Precision, attr.Scale), 64); err == nil {
					*iface = f64
				}
			}
		}
	case parquet.Type_INT32, parquet.Type_INT64:
		switch v := (*iface).(type) {
		case float64:
			*iface = v / math.Pow10(attr.Scale)
		case string:
			if f64, err := strconv.ParseFloat(v, 64); err == nil {
				*iface = f64 / math.Pow10(attr.Scale)
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
