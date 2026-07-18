package schema

import "testing"

func TestPtrEqual(t *testing.T) {
	one, anotherOne, two := 1, 1, 2
	tests := []struct {
		name string
		a    *int
		b    *int
		want bool
	}{
		{name: "both nil", want: true},
		{name: "left nil", b: &one},
		{name: "right nil", a: &one},
		{name: "equal", a: &one, b: &anotherOne, want: true},
		{name: "different", a: &one, b: &two},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ptrEqual(tt.a, tt.b); got != tt.want {
				t.Fatalf("ptrEqual() = %t, want %t", got, tt.want)
			}
		})
	}
}
