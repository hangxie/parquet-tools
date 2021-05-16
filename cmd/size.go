package cmd

import (
	"fmt"
)

// SizeCmd is a kong command for size
type SizeCmd struct {
	CommonOption
	Uncompressed bool `help:"Output uncompressed size." default:"false"`
}

// Run does actual size job
func (c *SizeCmd) Run(ctx *Context) error {
	reader, err := newParquetFileReader(c.URI)
	if err != nil {
		return err
	}

	compressedSize := int64(0)
	uncompressedSize := int64(0)
	for _, rg := range reader.Footer.RowGroups {
		for _, col := range rg.Columns {
			compressedSize += col.MetaData.TotalCompressedSize
			uncompressedSize += col.MetaData.TotalUncompressedSize
		}
	}

	if c.Uncompressed {
		fmt.Println(uncompressedSize)
	} else {
		fmt.Println(compressedSize)
	}
	return nil
}
