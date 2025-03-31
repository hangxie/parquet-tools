package cmd

import (
	"fmt"

	pio "github.com/hangxie/parquet-tools/internal/io"
)

// RowCountCmd is a kong command for rowcount
type RowCountCmd struct {
	pio.ReadOption
	URI string `arg:"" predictor:"file" help:"URI of Parquet file."`
}

// Run does actual rowcount job
func (c RowCountCmd) Run() error {
	reader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.PFile.Close()
	}()

	fmt.Println(reader.GetNumRows())
	return nil
}
