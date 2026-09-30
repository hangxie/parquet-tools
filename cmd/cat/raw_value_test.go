package cat

import (
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
	"github.com/stretchr/testify/require"
)

func TestRawValue(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Date, type=INT32, convertedtype=DATE"},{"Tag":"name=Blob, type=BYTE_ARRAY, convertedtype=BSON"},{"Tag":"name=Text, type=BYTE_ARRAY, convertedtype=UTF8"},{"Tag":"name=Optional, type=INT64, repetitiontype=OPTIONAL"}]}`)
	require.NoError(t, err)
	row := struct {
		Date     int32
		Blob     string
		Text     string
		Optional *int64
	}{Date: 123, Blob: "not BSON", Text: "SGVsbG8="}
	got, err := rawValue(reflect.ValueOf(row), handler.GetRootInName(), handler)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"Date": int32(123), "Blob": "bm90IEJTT04=", "Text": "SGVsbG8=", "Optional": nil}, got)
	_, err = rawValue(reflect.ValueOf(row), "missing", handler)
	require.ErrorContains(t, err, "schema path")
	got, err = rawValue(reflect.Value{}, "", handler)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRawValueInvalidWidth(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Id, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"}]}`)
	require.NoError(t, err)
	_, err = rawValue(reflect.ValueOf(struct{ Id string }{Id: "short"}), handler.GetRootInName(), handler)
	require.ErrorContains(t, err, "must be 16")
}

func TestRawValueContainers(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Id, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"}]}`)
	require.NoError(t, err)
	root := handler.GetRootInName()
	idPath := root + common.ParGoPathDelimiter + "Id"
	bytesValue := []byte("0123456789abcdef")
	got, err := rawValue(reflect.ValueOf(bytesValue), idPath, handler)
	require.NoError(t, err)
	require.Equal(t, base64.StdEncoding.EncodeToString(bytesValue), got)
	_, err = rawValue(reflect.ValueOf(1), root, handler)
	require.ErrorContains(t, err, "unexpected value")
	_, err = rawValue(reflect.ValueOf([]int32{1}), root, handler)
	require.ErrorContains(t, err, "schema path")
	_, err = rawValue(reflect.ValueOf(struct{ Other string }{"x"}), root, handler)
	require.ErrorContains(t, err, "schema path")
	handler.MapIndex[idPath] = int32(len(handler.SchemaElements))
	_, err = rawValue(reflect.ValueOf(bytesValue), idPath, handler)
	require.ErrorContains(t, err, "schema path")
}

func TestRawMapConversionErrors(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Entries, type=MAP","Fields":[{"Tag":"name=Key, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"},{"Tag":"name=Value, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"}]}]}`)
	require.NoError(t, err)
	path := handler.GetRootInName() + common.ParGoPathDelimiter + "Entries"
	for _, input := range []map[string]string{{"short": "0123456789abcdef"}, {"0123456789abcdef": "short"}} {
		_, err := rawValue(reflect.ValueOf(input), path, handler)
		require.ErrorContains(t, err, "must be 16")
	}
}

func TestRawVariantGroup(t *testing.T) {
	handler := &schema.SchemaHandler{MapIndex: map[string]int32{"Root": 0}, SchemaElements: []*parquet.SchemaElement{{LogicalType: &parquet.LogicalType{VARIANT: parquet.NewVariantType()}}}}
	value := types.Variant{Metadata: []byte{1, 0, 0}, Value: []byte{0}}
	got, err := rawValue(reflect.ValueOf(value), "Root", handler)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRawStructJSONTags(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Visible, type=INT32"}]}`)
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		value any
		want  map[string]any
	}{
		{"omitted field", struct {
			Visible int32
			Hidden  string `json:"-"`
		}{Visible: 7, Hidden: "secret"}, map[string]any{"Visible": int32(7)}},
		{"renamed field", struct {
			Visible int32 `json:"renamed,omitempty"`
		}{Visible: 7}, map[string]any{"renamed": int32(7)}},
		{"empty name", struct {
			Visible int32 `json:",omitempty"`
		}{Visible: 7}, map[string]any{"Visible": int32(7)}},
		{"omitted array", struct {
			Array []int32 `json:"-"`
		}{Array: []int32{1}}, map[string]any{}},
		{"unexported array", struct{ array []int32 }{array: []int32{1}}, map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rawValue(reflect.ValueOf(tc.value), handler.GetRootInName(), handler)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
