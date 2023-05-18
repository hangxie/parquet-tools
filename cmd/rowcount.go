package cmd

import (
	"fmt"

	"github.com/hangxie/parquet-tools/internal"
)

// RowCountCmd is a kong command for rowcount
type RowCountCmd struct {
	internal.ReadOption
}

// Run does actual rowcount job
func (c RowCountCmd) Run() error {
	reader, err := internal.NewParquetFileReader(c.ReadOption)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	fmt.Println(reader.GetNumRows())
	return nil
}
