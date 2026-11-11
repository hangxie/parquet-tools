package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	pqschema "github.com/hangxie/parquet-go/v3/schema"
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

func TestNewSchemaTreePathMapping(t *testing.T) {
	// JSON schema tags give each field distinct internal (Go-style) and external names.
	const jsonSchema = `{"Tag":"name=root","Fields":[
		{"Tag":"name=group_a","Fields":[{"Tag":"name=leaf_b, type=INT32"}]},
		{"Tag":"name=leaf_c, type=BYTE_ARRAY, convertedtype=UTF8"}]}`
	key := func(path ...string) string { return strings.Join(path, common.ParGoPathDelimiter) }

	testCases := map[string]struct {
		mutate func(*pqschema.SchemaHandler)
		errMsg string
	}{
		"valid-translation": {func(*pqschema.SchemaHandler) {}, ""},
		"nil-handler":       {nil, "schema handler is nil"},
		"missing-root": {func(sh *pqschema.SchemaHandler) {
			delete(sh.InPathToExPath, "Root")
		}, "no external path for element 0 [Root]"},
		"missing-child": {func(sh *pqschema.SchemaHandler) {
			delete(sh.InPathToExPath, key("Root", "Group_a", "Leaf_b"))
		}, "no external path for element 2 [Root.Group_a.Leaf_b]"},
		"renamed-element": {func(sh *pqschema.SchemaHandler) {
			sh.SchemaElements[3].Name = "Renamed"
		}, "no external path for element 3 [Root.Renamed]"},
		"wrong-depth": {func(sh *pqschema.SchemaHandler) {
			sh.InPathToExPath[key("Root", "Leaf_c")] = "root"
		}, "external path [root] of element 3 [Root.Leaf_c] does not extend parent path [root]"},
		"wrong-parent": {func(sh *pqschema.SchemaHandler) {
			sh.InPathToExPath[key("Root", "Group_a", "Leaf_b")] = key("root", "other", "leaf_b")
		}, "external path [root.other.leaf_b] of element 2 [Root.Group_a.Leaf_b] does not extend parent path [root.group_a]"},
		"empty-name": {func(sh *pqschema.SchemaHandler) {
			sh.InPathToExPath[key("Root", "Leaf_c")] = key("root", "")
		}, "external path [root.] of element 3 [Root.Leaf_c] has an empty name"},
		"changed-name": {func(sh *pqschema.SchemaHandler) {
			sh.InPathToExPath[key("Root", "Leaf_c")] = key("root", "bogus")
		}, "external name [bogus] of element 3 [Root.Leaf_c] does not match schema info [leaf_c]"},
		"changed-root-name": {func(sh *pqschema.SchemaHandler) {
			sh.InPathToExPath["Root"] = "bogus"
		}, "external name [bogus] of element 0 [Root] does not match schema info [root]"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			handler, err := pqschema.NewSchemaHandlerFromJSON(jsonSchema)
			require.NoError(t, err)
			pr := &reader.ParquetReader{SchemaHandler: handler, Footer: &parquet.FileMetaData{}}
			if tc.mutate == nil {
				pr.SchemaHandler = nil
			} else {
				tc.mutate(handler)
			}

			root, err := NewSchemaTree(context.Background(), pr, SchemaOption{SkipPageEncoding: true, SkipBloomFilter: true})
			if tc.errMsg != "" {
				require.Nil(t, root)
				require.EqualError(t, err, "malformed schema: "+tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "root", root.Name)
			require.Len(t, root.Children, 2)
			require.Equal(t, "group_a", root.Children[0].Name)
			require.Equal(t, []string{"Root", "Group_a"}, root.Children[0].InNamePath)
			require.Equal(t, []string{"root", "group_a", "leaf_b"}, root.Children[0].Children[0].ExNamePath)
			require.Equal(t, "leaf_c", root.Children[1].Name)
		})
	}
}

func TestNewSchemaTreeNestedPathMapping(t *testing.T) {
	// LIST and MAP layouts add synthetic nodes, and PARGO_PREFIX_ names translate; all must map cleanly.
	testCases := []string{
		"list-map-layout",
		"list-of-list",
		"map-value-map",
		"old-style-list",
		"nested-array-logical-list",
		"pargo-prefix-nested",
	}
	for _, name := range testCases {
		t.Run(name, func(t *testing.T) {
			pr, err := pio.NewParquetFileReader(context.Background(), "../testdata/"+name+".parquet", pio.ReadOption{})
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.PFile.Close()) }()

			root, err := NewSchemaTree(context.Background(), pr, SchemaOption{SkipPageEncoding: true, SkipBloomFilter: true})
			require.NoError(t, err)

			queue := []*SchemaNode{root}
			for len(queue) > 0 {
				node := queue[0]
				queue = append(queue[1:], node.Children...)
				require.NotEmpty(t, node.Name)
				require.Equal(t, node.Name, node.ExNamePath[len(node.ExNamePath)-1])
				require.Len(t, node.ExNamePath, len(node.InNamePath))
				for _, child := range node.Children {
					require.Equal(t, node.ExNamePath, child.ExNamePath[:len(node.ExNamePath)])
				}
			}
		})
	}
}
