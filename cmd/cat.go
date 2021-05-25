package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
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
	includeRow, err := c.includeRowFunc()
	if err != nil {
		return fmt.Errorf("unable to parse filter [%s]", c.Filter)
	}

	reader, err := newParquetFileReader(c.URI)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	// Output rows one by one to avoid running out of memory with a jumbo list
	fmt.Print("[")
	rand.Seed(time.Now().UnixNano())
	firstItem := true
	counter := int64(0)
	for counter < c.Limit {
		rows, err := reader.ReadByNumber(c.PageSize)
		if err != nil {
			return fmt.Errorf("failed to cat: %s", err)
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			buf, _ := json.Marshal(row)
			if !includeRow(buf) {
				continue
			}

			if rand.Float64() >= c.SampleRatio {
				continue
			}
			if firstItem {
				firstItem = false
			} else {
				fmt.Print(",")
			}

			fmt.Print(string(buf))

			counter += 1
			if counter >= c.Limit {
				break
			}
		}
	}
	fmt.Println("]")

	return nil
}

func (c *CatCmd) includeRowFunc() (func([]byte) bool, error) {
	if c.Filter == "" {
		return func([]byte) bool { return true }, nil
	}
	matches := regexp.MustCompile(`^ *(.*?) *([>=<]{1,2}) *(.*?) *$`).FindAllStringSubmatch(c.Filter, 3)
	if len(matches) == 0 {
		return nil, errors.New("unable to parse filter")
	}
	field := matches[0][1]
	operator := matches[0][2]
	value := matches[0][3]

	// TODO support >, >=, <, <=
	if operator != "==" && operator != "<>" {
		return nil, fmt.Errorf("invalid operator [%s]", operator)
	}
	if value == "" {
		return nil, errors.New("missing value in filter")
	}

	fieldList := strings.Split(field, ".")
	fieldCount := len(fieldList)
	return func(row []byte) bool {
		var rowObj map[string]interface{}
		if err := json.Unmarshal(row, &rowObj); err != nil {
			// this should not happen as the row string is what we marshalled
			panic("unable to parse row value as JSON")
		}

		for _, f := range fieldList[:(fieldCount - 1)] {
			if v, ok := rowObj[f].(map[string]interface{}); !ok {
				// row does not have nested layer deep enough
				return false
			} else {
				rowObj = v
			}
		}
		if _, ok := rowObj[fieldList[fieldCount-1]]; !ok {
			return false
		}
		valueToCompare := fmt.Sprint(rowObj[fieldList[fieldCount-1]])
		return (operator == "==" && value == valueToCompare) ||
			(operator == "<>" && value != valueToCompare)
	}, nil
}
