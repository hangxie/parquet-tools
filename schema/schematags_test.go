package schema

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestTypeStrVariant(t *testing.T) {
	node := parquet.SchemaElement{LogicalType: &parquet.LogicalType{VARIANT: &parquet.VariantType{}}}
	if got := typeStr(node); got != "VARIANT" {
		t.Fatalf("typeStr() = %q, want VARIANT", got)
	}
}

func TestUpdateTagForListNilRepetitionType(t *testing.T) {
	listType := parquet.ConvertedType_LIST
	node := SchemaNode{
		SchemaElement: parquet.SchemaElement{Name: "List", ConvertedType: &listType},
		Children: []*SchemaNode{
			{SchemaElement: parquet.SchemaElement{Name: "Element", LogicalType: &parquet.LogicalType{STRING: &parquet.StringType{}}}},
		},
	}
	tagMap := node.GetTagMap()
	if tagMap["convertedtype"] != "LIST" {
		t.Fatalf(`tagMap["convertedtype"] = %q, want LIST`, tagMap["convertedtype"])
	}
	if rt := node.Children[0].RepetitionType; rt == nil || *rt != parquet.FieldRepetitionType_REQUIRED {
		t.Fatalf("element repetition type = %v, want REQUIRED", rt)
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
