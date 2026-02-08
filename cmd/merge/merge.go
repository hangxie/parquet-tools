package merge

import (
	"context"
	"fmt"
	"regexp"
	"runtime"

	"github.com/hangxie/parquet-go/v2/reader"
	"golang.org/x/sync/errgroup"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

// Cmd is a kong command for merge
type Cmd struct {
	Concurrent   bool     `help:"enable concurrent processing" default:"false"`
	FailOnInt96  bool     `help:"fail command if INT96 data type is present." name:"fail-on-int96" default:"false"`
	ReadPageSize int      `help:"Page size to read from Parquet." default:"1000"`
	Source       []string `short:"s" help:"Files to be merged."`
	URI          string   `arg:"" predictor:"file" help:"URI of Parquet file."`
	pio.ReadOption
	pio.WriteOption
}

// Run does actual merge job
func (c Cmd) Run() (retErr error) {
	if c.ReadPageSize < 1 {
		return fmt.Errorf("invalid read page size %d, needs to be at least 1", c.ReadPageSize)
	}
	if len(c.Source) <= 1 {
		return fmt.Errorf("needs at least 2 source files")
	}

	fileReaders, schemaJSON, err := c.openSources()
	if err != nil {
		return err
	}
	defer func() {
		for _, fileReader := range fileReaders {
			_ = fileReader.PFile.Close()
		}
	}()

	fileWriter, err := pio.NewGenericWriter(c.URI, c.WriteOption, schemaJSON)
	if err != nil {
		return fmt.Errorf("failed to write to [%s]: %w", c.URI, err)
	}
	defer func() {
		if err := fileWriter.WriteStop(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to end write [%s]: %w", c.URI, err)
		}
		if err := fileWriter.PFile.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close [%s]: %w", c.URI, err)
		}
	}()

	// dedicated goroutine for output to ensure output integrity
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writerGroup, _ := errgroup.WithContext(ctx)
	writerChan := make(chan any)
	writerGroup.Go(func() error {
		return pio.PipelineWriter(ctx, fileWriter, writerChan, c.URI)
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
			return pio.PipelineReader(ctx, fileReaders[i], writerChan, c.Source[i], c.ReadPageSize, nil)
		})
	}

	if err := readerGroup.Wait(); err != nil {
		return err
	}
	close(writerChan)

	if err := writerGroup.Wait(); err != nil {
		return err
	}

	return nil
}

func (c Cmd) openSources() ([]*reader.ParquetReader, string, error) {
	var schemaJSON string
	var rootExNamePath []string
	var rootInNamePath []string
	var rootName string
	var err error
	fileReaders := make([]*reader.ParquetReader, len(c.Source))
	for i, source := range c.Source {
		fileReaders[i], err = pio.NewParquetFileReader(source, c.ReadOption)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read from [%s]: %w", source, err)
		}

		currSchema, err := pschema.NewSchemaTree(fileReaders[i], pschema.SchemaOption{FailOnInt96: c.FailOnInt96, WithCompressionCodec: true})
		if err != nil {
			return nil, "", err
		}

		if schemaJSON == "" {
			// Use schema from the first file (including its encodings)
			schemaJSON = currSchema.JSONSchema()
			rootName = currSchema.Name
			rootExNamePath = currSchema.ExNamePath
			rootInNamePath = currSchema.InNamePath
			continue
		}

		currSchema.Name = rootName
		currSchema.ExNamePath = rootExNamePath
		currSchema.InNamePath = rootInNamePath
		newSchema := currSchema.JSONSchema()
		// Strip encoding from both schemas for comparison as files may have different encodings
		schemaJSONWithoutEncoding := regexp.MustCompile(`, encoding=[A-Z_]+`).ReplaceAllString(schemaJSON, "")
		newSchemaWithoutEncoding := regexp.MustCompile(`, encoding=[A-Z_]+`).ReplaceAllString(newSchema, "")
		if newSchemaWithoutEncoding != schemaJSONWithoutEncoding {
			return nil, "", fmt.Errorf("[%s] does not have same schema as previous files", source)
		}
	}

	return fileReaders, schemaJSON, nil
}
