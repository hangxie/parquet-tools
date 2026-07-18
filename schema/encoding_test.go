package schema

import "testing"

func TestGetAllowedEncodingsReturnsCopy(t *testing.T) {
	first := GetAllowedEncodings("INT32")
	first[0] = "changed"
	second := GetAllowedEncodings("INT32")
	if second[0] != "PLAIN" {
		t.Fatalf("GetAllowedEncodings() reused mutable result: %q", second[0])
	}
}
