package merge

import (
	"context"
	"fmt"
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

	// Single errgroup so all goroutines share one derived context —
	// if either writer or any reader fails, gctx is cancelled and the other
	// side's select on ctx.Done() fires, preventing deadlock.
	g, gctx := errgroup.WithContext(context.Background())
	writerChan := make(chan any)

	g.Go(func() error {
		return pio.PipelineWriter(gctx, fileWriter, writerChan, c.URI)
	})

	g.Go(func() error {
		defer close(writerChan)
		readerGroup := new(errgroup.Group)
		if c.Concurrent {
			readerGroup.SetLimit(runtime.NumCPU())
		} else {
			readerGroup.SetLimit(1)
		}
		for i := range fileReaders {
			readerGroup.Go(func() error {
				return pio.PipelineReader(gctx, fileReaders[i], writerChan, c.Source[i], c.ReadPageSize, nil)
			})
		}
		return readerGroup.Wait()
	})

	return g.Wait()
}

func (c Cmd) openSources() ([]*reader.ParquetReader, string, error) {
	var schemaJSON string
	var rootSchema *pschema.SchemaNode
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

		if rootSchema == nil {
			rootSchema = currSchema
			// Build a separate tree for JSON since JSONSchema() mutates the tree
			jsonTree, jsonErr := pschema.NewSchemaTree(fileReaders[i], pschema.SchemaOption{FailOnInt96: c.FailOnInt96})
			if jsonErr != nil {
				return nil, "", jsonErr
			}
			schemaJSON = jsonTree.JSONSchema()
			continue
		}

		if !rootSchema.IsCompatible(currSchema, pschema.CompareOption{}) {
			return nil, "", fmt.Errorf("[%s] does not have same schema as previous files", source)
		}
	}

	return fileReaders, schemaJSON, nil
}
