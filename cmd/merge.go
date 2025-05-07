package cmd

import (
	"fmt"

	"github.com/hangxie/parquet-go/reader"

	pio "github.com/hangxie/parquet-tools/internal/io"
	pschema "github.com/hangxie/parquet-tools/internal/schema"
)

// MergeCmd is a kong command for merge
type MergeCmd struct {
	pio.ReadOption
	pio.WriteOption
	ReadPageSize int      `help:"Page size to read from Parquet." default:"1000"`
	Source       []string `short:"s" help:"Files to be merged."`
	URI          string   `arg:"" predictor:"file" help:"URI of Parquet file."`
	FailOnInt96  bool     `help:"fail command if INT96 data type presents." name:"fail-on-int96" default:"false"`
}

// Run does actual merge job
func (c MergeCmd) Run() error {
	if c.ReadPageSize < 1 {
		return fmt.Errorf("invalid read page size %d, needs to be at least 1", c.ReadPageSize)
	}
	if len(c.Source) <= 1 {
		return fmt.Errorf("needs at least 2 source files")
	}

	fileReaders, schemaJson, err := c.openSources()
	if err != nil {
		return err
	}
	defer func() {
		for _, fileReader := range fileReaders {
			_ = fileReader.PFile.Close()
		}
	}()

	fileWriter, err := pio.NewGenericWriter(c.URI, c.WriteOption, schemaJson)
	if err != nil {
		return fmt.Errorf("failed to write to [%s]: %w", c.URI, err)
	}
	defer func() {
		_ = fileWriter.WriteStop()
		_ = fileWriter.PFile.Close()
	}()

	for i := range fileReaders {
		for {
			rows, err := fileReaders[i].ReadByNumber(c.ReadPageSize)
			if err != nil {
				return fmt.Errorf("failed to read from [%s]: %w", c.Source[i], err)
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				if err := fileWriter.Write(row); err != nil {
					return fmt.Errorf("failed to write data from [%s] to [%s]: %w", c.Source[i], c.URI, err)
				}
			}
		}
	}
	if err := fileWriter.WriteStop(); err != nil {
		return fmt.Errorf("failed to end write [%s]: %w", c.URI, err)
	}
	if err := fileWriter.PFile.Close(); err != nil {
		return fmt.Errorf("failed to close [%s]: %w", c.URI, err)
	}

	return nil
}

func (c MergeCmd) openSources() ([]*reader.ParquetReader, string, error) {
	var schemaJson string
	var rootExNamePath []string
	var rootName string
	var err error
	fileReaders := make([]*reader.ParquetReader, len(c.Source))
	for i, source := range c.Source {
		fileReaders[i], err = pio.NewParquetFileReader(source, c.ReadOption)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read from [%s]: %w", source, err)
		}

		currSchema, err := pschema.NewSchemaTree(fileReaders[i], pschema.SchemaOption{FailOnInt96: c.FailOnInt96})
		if err != nil {
			return nil, "", err
		}

		if schemaJson == "" {
			schemaJson = currSchema.JSONSchema()
			rootName = currSchema.Name
			rootExNamePath = currSchema.ExNamePath
			continue
		}

		currSchema.Name = rootName
		currSchema.ExNamePath = rootExNamePath
		newSchema := currSchema.JSONSchema()
		if newSchema != schemaJson {
			return nil, "", fmt.Errorf("[%s] does not have same schema as previous files", source)
		}
	}

	return fileReaders, schemaJson, nil
}
