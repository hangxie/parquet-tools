package cmd

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// CatCmd is a kong command for cat
type CatCmd struct {
	CommonOption
	Limit    int64   `short:"l" help:"Max number of rows to output." default:"-1"`
	PageSize int     `short:"p" help:"Pagination size to read from Parquet." default:"1000"`
	Sampling float64 `short:"s" help:"Sampling percentage." default:"1.0"`
}

// Run does actual cat job
func (c *CatCmd) Run(ctx *Context) error {
	if c.Limit == -1 {
		c.Limit = int64(1<<63 - 1)
	}
	if c.Limit <= 0 {
		return fmt.Errorf("invalid limit %d, needs to be a positive number (-1 for unlimited)", c.Limit)
	}
	if c.PageSize < 1 {
		return fmt.Errorf("invalid page size %d, needs to be at least 1", c.PageSize)
	}
	// note that sampling rate at 0.0 is allowed, while it does not output anything
	if c.Sampling < 0.0 || c.Sampling > 1.0 {
		return fmt.Errorf("invalid sampling %f, needs to be between 0.0 and 1.0", c.Sampling)
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
			if rand.Float64() >= c.Sampling {
				continue
			}
			if firstItem {
				firstItem = false
			} else {
				fmt.Print(",")
			}

			buf, _ := json.Marshal(row)
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
