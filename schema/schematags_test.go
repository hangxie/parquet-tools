package schema

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"
)

func TestTypeStrVariant(t *testing.T) {
	node := parquet.SchemaElement{LogicalType: &parquet.LogicalType{VARIANT: &parquet.VariantType{}}}
	if got := typeStr(node); got != "VARIANT" {
		t.Fatalf("typeStr() = %q, want VARIANT", got)
	}
}

func TestTagUpdatesIgnoreAbsentMetadata(t *testing.T) {
	testCases := map[string]func(*SchemaNode, map[string]string){
		"empty-list":     (*SchemaNode).updateTagForList,
		"converted-type": (*SchemaNode).updateTagFromConvertedType,
		"logical-type":   (*SchemaNode).updateTagFromLogicalType,
	}

	for name, update := range testCases {
		t.Run(name, func(t *testing.T) {
			tags := map[string]string{}
			update(&SchemaNode{}, tags)
			require.Empty(t, tags)
		})
	}
}

func TestUpdateTagForMapShortKeyValue(t *testing.T) {
	mapType := parquet.ConvertedType_MAP
	keyValueType := parquet.ConvertedType_MAP_KEY_VALUE
	int32Type := parquet.Type_INT32
	node := SchemaNode{
		SchemaElement: parquet.SchemaElement{Name: "Map", ConvertedType: &mapType},
		Children: []*SchemaNode{
			{
				SchemaElement: parquet.SchemaElement{Name: "Key_value", ConvertedType: &keyValueType},
				Children: []*SchemaNode{
					{SchemaElement: parquet.SchemaElement{Name: "Key", Type: &int32Type}},
				},
			},
		},
	}
	tagMap := node.GetTagMap()
	if tagMap["convertedtype"] != "MAP" {
		t.Fatalf(`tagMap["convertedtype"] = %q, want MAP`, tagMap["convertedtype"])
	}
}
