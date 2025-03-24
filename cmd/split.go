package cmd

import (
	"fmt"

	"github.com/hangxie/parquet-go/reader"
	"github.com/hangxie/parquet-go/writer"

	"github.com/hangxie/parquet-tools/internal"
)

// current writer state
type TrunkWriter struct {
	fileIndex    int64
	targetFile   string
	writer       *writer.ParquetWriter
	recordCount  int64
	paddingCount int64
	schemaJSON   string
}

// SplitCmd is a kong command for split
type SplitCmd struct {
	internal.ReadOption
	internal.WriteOption
	ReadPageSize int    `help:"Page size to read from Parquet." default:"1000"`
	URI          string `arg:"" predictor:"file" help:"URI of Parquet file."`
	FileCount    int64  `xor:"RecordCount" help:"Generate this number of result files with potential empty ones"`
	RecordCount  int64  `xor:"FileCount" help:"Result files will have at most this number of records"`
	FailOnInt96  bool   `help:"Fail command if INT96 data type presents." name:"fail-on-int96" default:"false"`
	NameFormat   string `help:"Format to populate target file names" default:"result-%06d.parquet"`

	current TrunkWriter
}

func (c *SplitCmd) openReader() (*reader.ParquetReader, error) {
	parquetReader, err := internal.NewParquetFileReader(c.URI, c.ReadOption)
	if err != nil {
		return nil, fmt.Errorf("failed to open [%s]: %w", c.URI, err)
	}
	defer func() {
		_ = parquetReader.PFile.Close()
	}()
	schemaRoot, err := internal.NewSchemaTree(parquetReader, internal.SchemaOption{FailOnInt96: c.FailOnInt96})
	if err != nil {
		return nil, fmt.Errorf("failed to load schema for [%s]: %w", c.URI, err)
	}
	c.current.schemaJSON = schemaRoot.JSONSchema()

	if c.FileCount != 0 {
		c.RecordCount = parquetReader.GetNumRows() / c.FileCount
		c.current.paddingCount = parquetReader.GetNumRows() % c.RecordCount
	}

	return parquetReader, nil
}

func (c *SplitCmd) switchWriter() error {
	if c.current.writer != nil {
		if err := c.current.writer.WriteStop(); err != nil {
			return fmt.Errorf("failed to end write [%s]: %w", c.current.targetFile, err)
		}
		if err := c.current.writer.PFile.Close(); err != nil {
			return fmt.Errorf("failed to close [%s]: %w", c.current.targetFile, err)
		}
		c.current.writer = nil
	}

	var err error
	c.current.targetFile = fmt.Sprintf(c.NameFormat, c.current.fileIndex)
	c.current.writer, err = internal.NewGenericWriter(c.current.targetFile, c.WriteOption, c.current.schemaJSON)
	if err != nil {
		return fmt.Errorf("failed to write to [%s]: %w", c.current.targetFile, err)
	}
	c.current.fileIndex++
	if c.current.paddingCount != 0 {
		c.current.recordCount = -1
		c.current.paddingCount--
	} else {
		c.current.recordCount = 0
	}

	return nil
}

// Run does actual split job
func (c SplitCmd) Run() error {
	if c.ReadPageSize < 1 {
		return fmt.Errorf("invalid read page size %d, needs to be at least 1", c.ReadPageSize)
	}
	if c.FileCount == 0 && c.RecordCount == 0 {
		return fmt.Errorf("needs either --file-count or --record-count")
	}

	parquetReader, err := c.openReader()
	if err != nil {
		return err
	}

	c.current.fileIndex = 0
	c.current.targetFile = ""
	// this is to trigger open the first target file
	c.current.recordCount = c.RecordCount

	for {
		rows, err := parquetReader.ReadByNumber(c.ReadPageSize)
		if err != nil {
			return fmt.Errorf("failed to read from [%s]: %w", c.URI, err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if c.current.recordCount == c.RecordCount {
				if err := c.switchWriter(); err != nil {
					return err
				}
			}
			if err := c.current.writer.Write(row); err != nil {
				return fmt.Errorf("failed to write data from [%s]: %w", c.current.targetFile, err)
			}
			c.current.recordCount++
		}
	}
	if c.current.writer != nil {
		if err := c.current.writer.WriteStop(); err != nil {
			return fmt.Errorf("failed to end write [%s]: %w", c.current.targetFile, err)
		}
		if err := c.current.writer.PFile.Close(); err != nil {
			return fmt.Errorf("failed to close [%s]: %w", c.current.targetFile, err)
		}
	}

	return nil
}
