package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// CatCmd is a kong command for cat
type CatCmd struct {
	CommonOption
	Limit       int64   `short:"l" help:"Max number of rows to output, 0 means no limit." default:"0"`
	PageSize    int     `short:"p" help:"Pagination size to read from Parquet." default:"1000"`
	SampleRatio float64 `short:"s" help:"Sample ratio (0.0-1.0)." default:"1.0"`
	Filter      string  `short:"f" help:"Filter to apply, support == and <>."`
	Format      string  `help:"output format (json/stream)" enum:"json,stream" default:"json"`
}

// Run does actual cat job
func (c *CatCmd) Run(ctx *Context) error {
	if c.Limit < 0 {
		return fmt.Errorf("invalid limit %d, needs to be a positive number (0 for unlimited)", c.Limit)
	}
	if c.Limit == 0 {
		c.Limit = int64(1<<63 - 1)
	}
	if c.PageSize < 1 {
		return fmt.Errorf("invalid page size %d, needs to be at least 1", c.PageSize)
	}
	// note that sampling rate at 0.0 is allowed, while it does not output anything
	if c.SampleRatio < 0.0 || c.SampleRatio > 1.0 {
		return fmt.Errorf("invalid sampling %f, needs to be between 0.0 and 1.0", c.SampleRatio)
	}
	matchRow, err := c.matchRowFunc()
	if err != nil {
		return fmt.Errorf("unable to parse filter [%s]", c.Filter)
	}
	if c.Format != "json" && c.Format != "stream" {
		// should never reach here
		return fmt.Errorf("unknown format: %s", c.Format)
	}

	reader, err := newParquetFileReader(c.URI)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	delimiter := map[string]struct {
		begin string
		line  string
		end   string
	}{
		"json":   {"[", ",", "]"},
		"stream": {"", "\n", ""},
	}

	// Output rows one by one to avoid running out of memory with a jumbo list
	fmt.Print(delimiter[c.Format].begin)
	rand.Seed(time.Now().UnixNano())
	for counter := int64(0); counter < c.Limit; {
		rows, err := reader.ReadByNumber(c.PageSize)
		if err != nil {
			return fmt.Errorf("failed to cat: %s", err)
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			buf, _ := json.Marshal(row)
			matched, err := matchRow(buf)
			if err != nil {
				return fmt.Errorf("failed to run filter: %s", err.Error())
			}
			if !matched {
				continue
			}

			if rand.Float64() >= c.SampleRatio {
				continue
			}
			if counter != 0 {
				fmt.Print(delimiter[c.Format].line)
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

var supportedOperators = map[string]string{
	"==": "",
	"<>": "",
	">":  "",
	">=": "",
	"<":  "",
	"<=": "",
}

func (c *CatCmd) matchRowFunc() (func([]byte) (bool, error), error) {
	if c.Filter == "" {
		return func([]byte) (bool, error) { return true, nil }, nil
	}
	matches := regexp.MustCompile(`^ *(.*?) *([>=<]{1,2}) *(.*?) *$`).FindAllStringSubmatch(c.Filter, 3)
	if len(matches) == 0 {
		return nil, errors.New("unable to parse filter")
	}
	field := matches[0][1]
	operator := matches[0][2]
	value := matches[0][3]

	if _, ok := supportedOperators[operator]; !ok {
		return nil, fmt.Errorf("invalid operator [%s]", operator)
	}
	if value == "" {
		return nil, errors.New("missing value in filter")
	}

	// determine nil/null value
	valueIsNull := (strings.ToLower(value) == "nil" || strings.ToLower(value) == "null")

	// detemine string value
	valueIsString := false
	if value[0] == '"' || value[len(value)-1] == '"' {
		if len(value) == 1 {
			return nil, errors.New("single quote")
		}
		if value[0] != '"' {
			return nil, errors.New("missing leading quote")
		}
		if value[len(value)-1] != '"' {
			return nil, errors.New("missing trailing quote")
		}
		valueIsString = true
		value = value[1 : len(value)-1]
	}

	// determine numeric value
	valueFloat, err := strconv.ParseFloat(value, 64)
	if !valueIsNull && !valueIsString && err != nil {
		return nil, errors.New("not a numeric value")
	}

	fieldList := strings.Split(field, ".")
	fieldCount := len(fieldList)
	return func(row []byte) (bool, error) {
		var rowObj map[string]interface{}
		if err := json.Unmarshal(row, &rowObj); err != nil {
			// this should not happen as the row string is what we marshalled
			return false, errors.New("unable to parse JSON string")
		}

		for _, f := range fieldList[:(fieldCount - 1)] {
			v, ok := rowObj[f].(map[string]interface{})
			if !ok {
				// row does not have nested layer deep enough
				return operator == "<>", nil
			}
			rowObj = v
		}
		fieldValue, ok := rowObj[fieldList[fieldCount-1]]
		if !ok {
			// row does not have nested layer deep enough
			return operator == "<>", nil
		}

		if fieldValue == nil || valueIsNull {
			// nil only equals to nil
			switch operator {
			case "==":
				return fieldValue == nil && valueIsNull, nil
			case "<>":
				return !(fieldValue == nil && valueIsNull), nil
			}
			return false, nil
		}

		if v, ok := toNumber(fieldValue); ok {
			if valueIsString {
				// type mismatch also means value does not match
				return operator == "<>", nil
			}
			return (operator == "==" && v == valueFloat) ||
					(operator == "<>" && v != valueFloat) ||
					(operator == ">" && v > valueFloat) ||
					(operator == ">=" && v >= valueFloat) ||
					(operator == "<" && v < valueFloat) ||
					(operator == "<=" && v <= valueFloat),
				nil
		}

		if v, ok := fieldValue.(string); ok {
			if !valueIsString {
				// type mismatch also means value does not match
				return operator == "<>", nil
			}
			return (operator == "==" && v == value) ||
					(operator == "<>" && v != value) ||
					(operator == ">" && v > value) ||
					(operator == ">=" && v >= value) ||
					(operator == "<" && v < value) ||
					(operator == "<=" && v <= value),
				nil
		}

		// type mismatch also means value does not match
		return operator == "<>", nil
	}, nil
}
