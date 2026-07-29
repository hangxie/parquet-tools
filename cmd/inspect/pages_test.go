package inspect

import (
	"context"
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestConvertIndexPageHeader(t *testing.T) {
	page := (Cmd{}).convertPageHeaderInfo(reader.PageHeaderInfo{PageType: parquet.PageType_INDEX_PAGE}, nil)
	if page.Note != "Index page (column index)" {
		t.Fatalf("convertPageHeaderInfo() note = %q", page.Note)
	}
}

func TestPageReadsHonorCanceledContext(t *testing.T) {
	pr, err := pio.NewParquetFileReader(context.Background(), "../../testdata/dict-page.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = pr.PFile.Close() }()

	col := pr.Footer.RowGroups[0].Columns[0]
	pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
	schemaRoot, err := pschema.NewSchemaTree(context.Background(), pr, pschema.SchemaOption{SkipPageEncoding: true})
	require.NoError(t, err)
	schemaNode := schemaRoot.GetPathMap()[pathKey]

	pages, err := (Cmd{}).readPages(context.Background(), pr, 0, 0, schemaNode)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(pages), 2)
	require.Equal(t, parquet.PageType_DICTIONARY_PAGE, pages[0].Type)

	dataPageIndex := -1
	for i, page := range pages {
		if page.Type == parquet.PageType_DATA_PAGE || page.Type == parquet.PageType_DATA_PAGE_V2 {
			dataPageIndex = i
			break
		}
	}
	require.NotEqual(t, -1, dataPageIndex)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := map[string]func() error{
		"page headers": func() error {
			_, err := (Cmd{}).readPages(ctx, pr, 0, 0, schemaNode)
			return err
		},
		"dictionary values": func() error {
			_, err := (Cmd{}).readDictionaryPageValues(ctx, pr, col, schemaNode, pages[0])
			return err
		},
		"column values": func() error {
			_, err := (Cmd{}).readPageValues(ctx, pr, 0, 0, col, schemaNode, pages, dataPageIndex)
			return err
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test()
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
