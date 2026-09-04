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

func TestTagUpdatesOnMalformedStructures(t *testing.T) {
	mapKeyValue := parquet.ConvertedTypePtr(parquet.ConvertedType_MAP_KEY_VALUE)

	testCases := map[string]struct {
		node     *SchemaNode
		update   func(*SchemaNode, map[string]string)
		wantTags map[string]string
	}{
		"list-nil-child": {
			node:     &SchemaNode{Children: []*SchemaNode{nil}},
			update:   (*SchemaNode).updateTagForList,
			wantTags: map[string]string{},
		},
		"list-element-without-repetition-type": {
			node: &SchemaNode{Children: []*SchemaNode{
				{Children: []*SchemaNode{{}}},
			}},
			update:   (*SchemaNode).updateTagForList,
			wantTags: map[string]string{},
		},
		"list-logical-type-element-not-repeated": {
			node: &SchemaNode{Children: []*SchemaNode{
				{SchemaElement: parquet.SchemaElement{
					Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
					LogicalType:    &parquet.LogicalType{STRING: &parquet.StringType{}},
					RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
				}},
			}},
			update:   (*SchemaNode).updateTagForList,
			wantTags: map[string]string{},
		},
		"list-legacy-scalar-element": {
			node: &SchemaNode{Children: []*SchemaNode{
				{SchemaElement: parquet.SchemaElement{
					Type:           parquet.TypePtr(parquet.Type_INT32),
					RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
				}},
			}},
			update:   (*SchemaNode).updateTagForList,
			wantTags: map[string]string{"valuetype": "INT32", "valuerepetitiontype": "REQUIRED"},
		},
		"list-scalar-element-not-repeated": {
			node: &SchemaNode{Children: []*SchemaNode{
				{SchemaElement: parquet.SchemaElement{
					Type:           parquet.TypePtr(parquet.Type_INT32),
					RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
				}},
			}},
			update:   (*SchemaNode).updateTagForList,
			wantTags: map[string]string{},
		},
		"map-nil-child": {
			node:     &SchemaNode{Children: []*SchemaNode{nil}},
			update:   (*SchemaNode).updateTagForMap,
			wantTags: map[string]string{},
		},
		"map-key-value-without-value": {
			node: &SchemaNode{Children: []*SchemaNode{
				{
					SchemaElement: parquet.SchemaElement{ConvertedType: mapKeyValue},
					Children:      []*SchemaNode{{}},
				},
			}},
			update:   (*SchemaNode).updateTagForMap,
			wantTags: map[string]string{},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tags := map[string]string{}
			require.NotPanics(t, func() { tc.update(tc.node, tags) })
			require.Equal(t, tc.wantTags, tags)
		})
	}
}
