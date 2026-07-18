package retype

import "testing"

func TestConverterWithoutDataRulesReturnsInput(t *testing.T) {
	converter := NewConverter([]*RetypeRule{{Name: "schema-only"}}, []map[string]struct{}{{}})
	input := struct{ Value string }{Value: "unchanged"}
	got, err := converter.Convert(input)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	if got != input {
		t.Fatalf("Convert() = %#v, want %#v", got, input)
	}
}
