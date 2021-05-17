package cmd

import (
	"fmt"
)

var (
	queryRaw          string = "raw"
	queryUncompressed string = "uncompressed"
	queryFooter       string = "footer"
	queryAll          string = "all"
)

// SizeCmd is a kong command for size
type SizeCmd struct {
	CommonOption
	Query string `short:"q" help:"Output size." enum:"raw,uncompressed,footer,all" default:"raw"`
}

// Run does actual size job
func (c *SizeCmd) Run(ctx *Context) error {
	reader, err := newParquetFileReader(c.URI)
	if err != nil {
		return err
	}
	defer reader.PFile.Close()

	footerSize, err := reader.GetFooterSize()
	if err != nil {
		return err
	}
	if c.Query == queryFooter {
		fmt.Println(footerSize)
		return nil
	}

	compressedSize := int64(0)
	uncompressedSize := int64(0)
	for _, rg := range reader.Footer.RowGroups {
		for _, col := range rg.Columns {
			compressedSize += col.MetaData.TotalCompressedSize
			uncompressedSize += col.MetaData.TotalUncompressedSize
		}
	}

	switch c.Query {
	case queryRaw:
		fmt.Println(compressedSize)
	case queryUncompressed:
		fmt.Println(uncompressedSize)
	case queryAll:
		fmt.Println(compressedSize, uncompressedSize, footerSize)
	default:
		return fmt.Errorf("unknown query type: %s", c.Query)
	}
	return nil
}
