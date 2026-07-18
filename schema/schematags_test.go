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
