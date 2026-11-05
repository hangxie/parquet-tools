package schema

import (
	"context"
	"testing"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestNewSchemaTreeMalformedSchema(t *testing.T) {
	testCases := map[string]struct {
		mutate func([]*parquet.SchemaElement) []*parquet.SchemaElement
		errMsg string
	}{
		"nil-root": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement {
			s[0] = nil
			return s
		}, "element 0 is nil"},
		"nil-child": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement {
			s[1] = nil
			return s
		}, "element 1 is nil"},
		"negative-root-count": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement {
			s[0].NumChildren = common.ToPtr(int32(-1))
			return s
		}, "negative num_children"},
		"negative-child-count": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement {
			s[1].NumChildren = common.ToPtr(int32(-1))
			return s
		}, "negative num_children"},
		"trailing-leaf": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement {
			return append(s, &parquet.SchemaElement{Name: "extra", Type: common.ToPtr(parquet.Type_INT32)})
		}, "outside the root subtree"},
		"trailing-group": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement {
			return append(s, &parquet.SchemaElement{Name: "extra", NumChildren: common.ToPtr(int32(0))})
		}, "outside the root subtree"},
		"root-too-few": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement {
			s[0].NumChildren = common.ToPtr(s[0].GetNumChildren() - 1)
			return s
		}, "outside the root subtree"},
		"no-elements":      {func(s []*parquet.SchemaElement) []*parquet.SchemaElement { return nil }, "schema has no elements"},
		"root-only":        {func(s []*parquet.SchemaElement) []*parquet.SchemaElement { return s[:1] }, "declares"},
		"missing-last-one": {func(s []*parquet.SchemaElement) []*parquet.SchemaElement { return s[:len(s)-1] }, "declares"},
	}

	// A cancelled context fails any metadata read, so it must not mask the schema error.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	runs := map[string]struct {
		ctx    context.Context
		option SchemaOption
	}{
		"skip-reads":       {context.Background(), SchemaOption{SkipPageEncoding: true, SkipBloomFilter: true}},
		"default":          {context.Background(), SchemaOption{}},
		"default-canceled": {cancelled, SchemaOption{}},
	}

	for name, tc := range testCases {
		for runName, run := range runs {
			t.Run(name+"/"+runName, func(t *testing.T) {
				pr, err := pio.NewParquetFileReader(context.Background(), "../testdata/good.parquet", pio.ReadOption{})
				require.NoError(t, err)
				defer func() { require.NoError(t, pr.PFile.Close()) }()

				pr.SchemaHandler.SchemaElements = tc.mutate(pr.SchemaHandler.SchemaElements)
				root, err := NewSchemaTree(run.ctx, pr, run.option)
				require.Nil(t, root)
				require.ErrorContains(t, err, tc.errMsg)
			})
		}
	}
}
