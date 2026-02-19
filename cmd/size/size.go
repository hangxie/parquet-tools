package size

import (
	"encoding/json"
	"fmt"

	pio "github.com/hangxie/parquet-tools/io"
)

const (
	queryRaw          = "raw"
	queryUncompressed = "uncompressed"
	queryFooter       = "footer"
	queryAll          = "all"
)

// Cmd is a kong command for size
type Cmd struct {
	Query string `short:"q" help:"Size to query (raw/uncompressed/footer/all)." enum:"raw,uncompressed,footer,all" default:"raw"`
	JSON  bool   `short:"j" help:"Output in JSON format." default:"false"`
	URI   string `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
}

// Run does actual size job
func (c Cmd) Run() error {
	reader, err := pio.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.PFile.Close()
	}()

	footerSize, err := reader.GetFooterSize()
	if err != nil {
		return err
	}

	rawSize := int64(0)
	uncompressedSize := int64(0)
	if c.Query != queryFooter {
		// do not scan all row groups whenever we are asked for footer size only
		for _, rg := range reader.Footer.RowGroups {
			for _, col := range rg.Columns {
				rawSize += col.MetaData.TotalCompressedSize
				uncompressedSize += col.MetaData.TotalUncompressedSize
			}
		}
	}

	var size struct {
		Raw          *int64  `json:",omitempty"`
		Uncompressed *int64  `json:",omitempty"`
		Footer       *uint32 `json:",omitempty"`
	}

	switch c.Query {
	case queryRaw:
		if !c.JSON {
			fmt.Println(rawSize)
			return nil
		}
		size.Raw = &rawSize
	case queryUncompressed:
		if !c.JSON {
			fmt.Println(uncompressedSize)
			return nil
		}
		size.Uncompressed = &uncompressedSize
	case queryFooter:
		if !c.JSON {
			fmt.Println(footerSize)
			return nil
		}
		size.Footer = &footerSize
	case queryAll:
		if !c.JSON {
			fmt.Println(rawSize, uncompressedSize, footerSize)
			return nil
		}
		size.Footer = &footerSize
		size.Raw = &rawSize
		size.Uncompressed = &uncompressedSize
	default:
		return fmt.Errorf("unknown query type: [%s]", c.Query)
	}

	buf, _ := json.Marshal(size)
	fmt.Println(string(buf))

	return nil
}
