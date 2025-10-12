package cmd

import (
	"context"
	"fmt"
	"runtime"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/writer"
	"golang.org/x/sync/errgroup"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// MergeCmd is a kong command for merge
type MergeCmd struct {
	Concurrent   bool     `help:"enable concurrent processing" default:"false"`
	FailOnInt96  bool     `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
	ReadPageSize int      `help:"Page size to read from Parquet." default:"1000"`
	Source       []string `short:"s" help:"Files to be merged."`
	URI          string   `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
	pio.WriteOption
}

func (c MergeCmd) writer(ctx context.Context, fileWriter *writer.ParquetWriter, writerChan chan any) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, more := <-writerChan
			if !more {
				return nil
			}
			if err := fileWriter.Write(row); err != nil {
				return fmt.Errorf("failed to write data from to [%s]: %w", c.URI, err)
			}
		}
	}
}

func (c MergeCmd) reader(ctx context.Context, source string, fileReader *reader.ParquetReader, writerChan chan any) error {
	for {
		rows, err := fileReader.ReadByNumber(c.ReadPageSize)
		if err != nil {
			return fmt.Errorf("failed to read from [%s]: %w", source, err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				writerChan <- row
			}
		}
	}
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

	// dedicated goroutine for output to ensure output integrity
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writerGroup, _ := errgroup.WithContext(ctx)
	writerChan := make(chan any)
	writerGroup.Go(func() error {
		return c.writer(ctx, fileWriter, writerChan)
	})

	var concurrencyChan chan struct{}
	if c.Concurrent {
		concurrencyChan = make(chan struct{}, runtime.NumCPU())
	} else {
		concurrencyChan = make(chan struct{}, 1)
	}

	readerGroup, _ := errgroup.WithContext(ctx)
	for i := range fileReaders {
		i := i
		concurrencyChan <- struct{}{}
		readerGroup.Go(func() error {
			defer func() {
				<-concurrencyChan
			}()
			return c.reader(ctx, c.Source[i], fileReaders[i], writerChan)
		})
	}

	if err := readerGroup.Wait(); err != nil {
		return err
	}
	close(writerChan)

	if err := writerGroup.Wait(); err != nil {
		return err
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
