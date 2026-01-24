package rowcount

import (
	"fmt"

	pio "github.com/hangxie/parquet-tools/io"
)

// Cmd is a kong command for rowcount
type Cmd struct {
	URI string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
}

// Run does actual rowcount job
func (c Cmd) Run() error {
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
