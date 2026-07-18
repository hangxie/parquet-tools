package schema

import "testing"

func TestSchemaOptionZeroValue(t *testing.T) {
	option := SchemaOption{}
	if option.FailOnInt96 || option.SkipPageEncoding {
		t.Fatal("zero-value schema option must leave optional behavior disabled")
	}
}
