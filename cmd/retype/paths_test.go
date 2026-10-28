package retype

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"

	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestNodePath(t *testing.T) {
	testCases := map[string]struct {
		inNamePath []string
		expected   string
	}{
		"root":   {[]string{"Parquet_go_root"}, ""},
		"empty":  {nil, ""},
		"top":    {[]string{"Parquet_go_root", "A"}, "A"},
		"nested": {[]string{"Parquet_go_root", "A", "Ts"}, "A\x01Ts"},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, nodePath(&pschema.SchemaNode{InNamePath: tc.inNamePath}))
		})
	}
}

func TestInName(t *testing.T) {
	require.Equal(t, "", inName(&pschema.SchemaNode{}))
	require.Equal(t, "Ts", inName(&pschema.SchemaNode{InNamePath: []string{"Parquet_go_root", "A", "Ts"}}))
}

func TestChildPath(t *testing.T) {
	require.Equal(t, "A", childPath("", "A"))
	require.Equal(t, "A\x01Ts", childPath("A", "Ts"))
}

func TestMapValuePath(t *testing.T) {
	require.Equal(t, "M\x01Key_value\x01Value", mapValuePath("M"))
}

func TestDisplayPath(t *testing.T) {
	require.Equal(t, "a.List.Element", displayPath("a\x01List\x01Element"))
}

func TestCollectListPaths(t *testing.T) {
	repeated := parquet.FieldRepetitionType_REPEATED
	required := parquet.FieldRepetitionType_REQUIRED
	listCT := parquet.ConvertedType_LIST
	node := func(rep *parquet.FieldRepetitionType, path ...string) *pschema.SchemaNode {
		return &pschema.SchemaNode{
			SchemaElement: parquet.SchemaElement{RepetitionType: rep},
			InNamePath:    append([]string{"Parquet_go_root"}, path...),
		}
	}
	list := func(name, wrapper, element string, elementRep *parquet.FieldRepetitionType) *pschema.SchemaNode {
		group := node(&required, name)
		group.ConvertedType = &listCT
		w := node(&repeated, name, wrapper)
		w.Children = []*pschema.SchemaNode{node(elementRep, name, wrapper, element)}
		group.Children = []*pschema.SchemaNode{w}
		return group
	}

	logical := list("Logical", "List", "Element", &required)
	logical.ConvertedType = nil
	logical.LogicalType = &parquet.LogicalType{LIST: &parquet.ListType{}}
	unannotated := list("Plain", "List", "Element", &required)
	unannotated.ConvertedType = nil
	notRepeatedWrapper := list("Wrapper", "List", "Element", &required)
	notRepeatedWrapper.Children[0].RepetitionType = &required

	root := node(nil)
	root.Children = []*pschema.SchemaNode{
		list("Standard", "List", "Element", &required),
		logical,
		unannotated,
		notRepeatedWrapper,
		list("Array", "Array", "Element", &required),
		list("Legacy", "List", "Element", &repeated),
		list("Named", "List", "Item", &required),
		node(&repeated, "Repeated"),
	}

	require.Equal(t, map[string]struct{}{"Standard": {}, "Logical": {}}, collectListPaths(root))
}
