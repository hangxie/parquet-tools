package cmd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
)

func Test_SchemaCmd_typeStr_both_nil(t *testing.T) {
	assert.Equal(t, "STRUCT", typeStr(parquet.SchemaElement{Type: nil, ConvertedType: nil}))
}

func Test_SchemaCmd_typeStr_converted_type_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	assert.Equal(t, "FIXED_LEN_BYTE_ARRAY", typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: nil}))
}

func Test_SchemaCmd_typeStr_type_nil(t *testing.T) {
	cType := parquet.ConvertedType_LIST
	assert.Equal(t, "LIST", typeStr(parquet.SchemaElement{Type: nil, ConvertedType: &cType}))
}

func Test_SchemaCmd_typeStr_both_non_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	cType := parquet.ConvertedType_LIST
	assert.Equal(t, "FIXED_LEN_BYTE_ARRAY", typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: &cType}))
}

func Test_SchemaCmd_repetitionTyeStr_good(t *testing.T) {
	assert.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: nil}))

	rType := parquet.FieldRepetitionType_OPTIONAL
	assert.Equal(t, "OPTIONAL", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REQUIRED
	assert.Equal(t, "REQUIRED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))

	rType = parquet.FieldRepetitionType_REPEATED
	assert.Equal(t, "REPEATED", repetitionTyeStr(parquet.SchemaElement{RepetitionType: &rType}))
}

func Test_SchemaCmd_goTypeStr_good(t *testing.T) {
	assert.Equal(t, "", goTypeStr(parquet.SchemaElement{Type: nil}))

	pType := parquet.Type_BOOLEAN
	assert.Equal(t, "bool", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT32
	assert.Equal(t, "int32", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT64
	assert.Equal(t, "int64", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_INT96
	assert.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_FLOAT
	assert.Equal(t, "float32", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_DOUBLE
	assert.Equal(t, "float64", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_BYTE_ARRAY
	assert.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))

	pType = parquet.Type_FIXED_LEN_BYTE_ARRAY
	assert.Equal(t, "string", goTypeStr(parquet.SchemaElement{Type: &pType}))
}

func Test_SchemaCmd_Run_non_existent(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "file/does/not/exist",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open local file")
}

func Test_SchemaCmd_Run_invalid_format(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "invalid",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unknown schema format")
}

func Test_SchemaCmd_Run_good_raw(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "raw",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"repetition_type":"REQUIRED","name":"Parquet_go_root","num_children":39,"children":[{"type":"BOOLEAN","type_length":0,"repetition_type":"REQUIRED","name":"Bool","scale":0,"precision":0,"field_id":0},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int32","scale":0,"precision":0,"field_id":0},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Int64","scale":0,"precision":0,"field_id":0},{"type":"INT96","type_length":0,"repetition_type":"REQUIRED","name":"Int96","scale":0,"precision":0,"field_id":0},{"type":"FLOAT","type_length":0,"repetition_type":"REQUIRED","name":"Float","scale":0,"precision":0,"field_id":0},{"type":"DOUBLE","type_length":0,"repetition_type":"REQUIRED","name":"Double","scale":0,"precision":0,"field_id":0},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Bytearray","scale":0,"precision":0,"field_id":0},{"type":"FIXED_LEN_BYTE_ARRAY","type_length":10,"repetition_type":"REQUIRED","name":"FixedLenByteArray","scale":0,"precision":0,"field_id":0},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Utf8","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int_8","converted_type":"INT_8","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":8,"isSigned":true}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int_16","converted_type":"INT_16","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":16,"isSigned":true}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int_32","converted_type":"INT_32","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":32,"isSigned":true}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Int_64","converted_type":"INT_64","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":64,"isSigned":true}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Uint_8","converted_type":"UINT_8","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":8,"isSigned":false}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Uint_16","converted_type":"UINT_16","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":16,"isSigned":false}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Uint_32","converted_type":"UINT_32","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":32,"isSigned":false}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Uint_64","converted_type":"UINT_64","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":64,"isSigned":false}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Date","converted_type":"DATE","scale":0,"precision":0,"field_id":0,"logicalType":{"DATE":{}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Date2","converted_type":"DATE","scale":0,"precision":0,"field_id":0,"logicalType":{"DATE":{}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Timemillis","converted_type":"TIME_MILLIS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":false,"unit":{"MILLIS":{}}}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Timemillis2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":true,"unit":{"MILLIS":{}}}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timemicros","converted_type":"TIME_MICROS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timemicros2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmillis","converted_type":"TIMESTAMP_MILLIS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":false,"unit":{"MILLIS":{}}}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmillis2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":true,"unit":{"MILLIS":{}}}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmicros","converted_type":"TIMESTAMP_MICROS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmicros2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}}},{"type":"FIXED_LEN_BYTE_ARRAY","type_length":12,"repetition_type":"REQUIRED","name":"Interval","converted_type":"INTERVAL","scale":0,"precision":0,"field_id":0},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Decimal1","converted_type":"DECIMAL","scale":2,"precision":9,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":9}}},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Decimal2","converted_type":"DECIMAL","scale":2,"precision":18,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":18}}},{"type":"FIXED_LEN_BYTE_ARRAY","type_length":12,"repetition_type":"REQUIRED","name":"Decimal3","converted_type":"DECIMAL","scale":2,"precision":10,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":10}}},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Decimal4","converted_type":"DECIMAL","scale":2,"precision":20,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":20}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Decimal5","scale":2,"precision":9,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":9}}},{"type":"FIXED_LEN_BYTE_ARRAY","type_length":12,"repetition_type":"OPTIONAL","name":"Decimal_pointer","converted_type":"DECIMAL","scale":2,"precision":10,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":10}}},{"repetition_type":"REQUIRED","name":"Map","num_children":1,"converted_type":"MAP","children":[{"repetition_type":"REPEATED","name":"Key_value","num_children":2,"converted_type":"MAP_KEY_VALUE","children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Key","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}}},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Value","scale":0,"precision":0,"field_id":0}]}]},{"repetition_type":"REQUIRED","name":"List","num_children":1,"converted_type":"LIST","children":[{"repetition_type":"REPEATED","name":"List","num_children":1,"children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Element","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}}}]}]},{"type":"INT32","type_length":0,"repetition_type":"REPEATED","name":"Repeated","scale":0,"precision":0,"field_id":0},{"repetition_type":"REQUIRED","name":"NestedMap","num_children":1,"converted_type":"MAP","children":[{"repetition_type":"REPEATED","name":"Key_value","num_children":2,"converted_type":"MAP_KEY_VALUE","children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Key","scale":0,"precision":0,"field_id":0},{"repetition_type":"REQUIRED","name":"Value","num_children":2,"children":[{"repetition_type":"REQUIRED","name":"Map","num_children":1,"converted_type":"MAP","children":[{"repetition_type":"REPEATED","name":"Key_value","num_children":2,"converted_type":"MAP_KEY_VALUE","children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Key","scale":0,"precision":0,"field_id":0},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Value","scale":0,"precision":0,"field_id":0}]}]},{"repetition_type":"REQUIRED","name":"List","num_children":1,"converted_type":"LIST","children":[{"repetition_type":"REPEATED","name":"List","num_children":1,"children":[{"type":"BYTE_ARRAY","type_length":12,"repetition_type":"REQUIRED","name":"Element","converted_type":"DECIMAL","scale":2,"precision":10,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":10}}}]}]}]}]}]},{"repetition_type":"REQUIRED","name":"NestedList","num_children":1,"converted_type":"LIST","children":[{"repetition_type":"REPEATED","name":"List","num_children":1,"children":[{"repetition_type":"REQUIRED","name":"Element","num_children":2,"children":[{"repetition_type":"REQUIRED","name":"Map","num_children":1,"converted_type":"MAP","children":[{"repetition_type":"REPEATED","name":"Key_value","num_children":2,"converted_type":"MAP_KEY_VALUE","children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Key","scale":0,"precision":0,"field_id":0},{"type":"BYTE_ARRAY","type_length":12,"repetition_type":"REQUIRED","name":"Value","converted_type":"DECIMAL","scale":2,"precision":10,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":10}}}]}]},{"repetition_type":"REQUIRED","name":"List","num_children":1,"converted_type":"LIST","children":[{"repetition_type":"REPEATED","name":"List","num_children":1,"children":[{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Element","scale":0,"precision":0,"field_id":0}]}]}]}]}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_json(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"Tag":"name=Parquet_go_root, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Bool, type=BOOLEAN, repetitiontype=REQUIRED"},{"Tag":"name=Int32, type=INT32, repetitiontype=REQUIRED"},{"Tag":"name=Int64, type=INT64, repetitiontype=REQUIRED"},{"Tag":"name=Int96, type=INT96, repetitiontype=REQUIRED"},{"Tag":"name=Float, type=FLOAT, repetitiontype=REQUIRED"},{"Tag":"name=Double, type=DOUBLE, repetitiontype=REQUIRED"},{"Tag":"name=Bytearray, type=BYTE_ARRAY, repetitiontype=REQUIRED"},{"Tag":"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10, repetitiontype=REQUIRED"},{"Tag":"name=Utf8, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},{"Tag":"name=Int_8, type=INT32, convertedtype=INT_8, repetitiontype=REQUIRED"},{"Tag":"name=Int_16, type=INT32, convertedtype=INT_16, repetitiontype=REQUIRED"},{"Tag":"name=Int_32, type=INT32, convertedtype=INT_32, repetitiontype=REQUIRED"},{"Tag":"name=Int_64, type=INT64, convertedtype=INT_64, repetitiontype=REQUIRED"},{"Tag":"name=Uint_8, type=INT32, convertedtype=UINT_8, repetitiontype=REQUIRED"},{"Tag":"name=Uint_16, type=INT32, convertedtype=UINT_16, repetitiontype=REQUIRED"},{"Tag":"name=Uint_32, type=INT32, convertedtype=UINT_32, repetitiontype=REQUIRED"},{"Tag":"name=Uint_64, type=INT64, convertedtype=UINT_64, repetitiontype=REQUIRED"},{"Tag":"name=Date, type=INT32, convertedtype=DATE, repetitiontype=REQUIRED"},{"Tag":"name=Date2, type=INT32, convertedtype=DATE, repetitiontype=REQUIRED"},{"Tag":"name=Timemillis, type=INT32, convertedtype=TIME_MILLIS, repetitiontype=REQUIRED"},{"Tag":"name=Timemillis2, type=INT32, repetitiontype=REQUIRED"},{"Tag":"name=Timemicros, type=INT64, convertedtype=TIME_MICROS, repetitiontype=REQUIRED"},{"Tag":"name=Timemicros2, type=INT64, repetitiontype=REQUIRED"},{"Tag":"name=Timestampmillis, type=INT64, convertedtype=TIMESTAMP_MILLIS, repetitiontype=REQUIRED"},{"Tag":"name=Timestampmillis2, type=INT64, repetitiontype=REQUIRED"},{"Tag":"name=Timestampmicros, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=REQUIRED"},{"Tag":"name=Timestampmicros2, type=INT64, repetitiontype=REQUIRED"},{"Tag":"name=Interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, repetitiontype=REQUIRED"},{"Tag":"name=Decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9, repetitiontype=REQUIRED"},{"Tag":"name=Decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18, repetitiontype=REQUIRED"},{"Tag":"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=REQUIRED"},{"Tag":"name=Decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20, repetitiontype=REQUIRED"},{"Tag":"name=Decimal5, type=INT32, repetitiontype=REQUIRED"},{"Tag":"name=Decimal_pointer, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=OPTIONAL"},{"Tag":"name=Map, type=MAP, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Key, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},{"Tag":"name=Value, type=INT32, repetitiontype=REQUIRED"}]},{"Tag":"name=List, type=LIST, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Element, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"}]},{"Tag":"name=Repeated, type=INT32, repetitiontype=REPEATED"},{"Tag":"name=NestedMap, type=MAP, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Key, type=BYTE_ARRAY, repetitiontype=REQUIRED"},{"Tag":"name=Value, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Map, type=MAP, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Key, type=BYTE_ARRAY, repetitiontype=REQUIRED"},{"Tag":"name=Value, type=INT32, repetitiontype=REQUIRED"}]},{"Tag":"name=List, type=LIST, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Element, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, repetitiontype=REQUIRED"}]}]}]},{"Tag":"name=NestedList, type=LIST, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Element, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Map, type=MAP, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Key, type=BYTE_ARRAY, repetitiontype=REQUIRED"},{"Tag":"name=Value, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, repetitiontype=REQUIRED"}]},{"Tag":"name=List, type=LIST, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Element, type=INT32, repetitiontype=REQUIRED"}]}]}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_SchemaCmd_Run_good_go(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "file://./testdata/all-types.parquet",
		},
		Format: "go",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})

	assert.Equal(t,
		strings.Join([]string{
			"type Parquet_go_root struct {",
			"Bool bool `parquet:\"name=Bool, type=BOOLEAN, repetitiontype=REQUIRED\"`",
			"Int32 int32 `parquet:\"name=Int32, type=INT32, repetitiontype=REQUIRED\"`",
			"Int64 int64 `parquet:\"name=Int64, type=INT64, repetitiontype=REQUIRED\"`",
			"Int96 string `parquet:\"name=Int96, type=INT96, repetitiontype=REQUIRED\"`",
			"Float float32 `parquet:\"name=Float, type=FLOAT, repetitiontype=REQUIRED\"`",
			"Double float64 `parquet:\"name=Double, type=DOUBLE, repetitiontype=REQUIRED\"`",
			"Bytearray string `parquet:\"name=Bytearray, type=BYTE_ARRAY, repetitiontype=REQUIRED\"`",
			"FixedLenByteArray string `parquet:\"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10, repetitiontype=REQUIRED\"`",
			"Utf8 string `parquet:\"name=Utf8, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED\"`",
			"Int_8 int32 `parquet:\"name=Int_8, type=INT32, convertedtype=INT_8, repetitiontype=REQUIRED\"`",
			"Int_16 int32 `parquet:\"name=Int_16, type=INT32, convertedtype=INT_16, repetitiontype=REQUIRED\"`",
			"Int_32 int32 `parquet:\"name=Int_32, type=INT32, convertedtype=INT_32, repetitiontype=REQUIRED\"`",
			"Int_64 int64 `parquet:\"name=Int_64, type=INT64, convertedtype=INT_64, repetitiontype=REQUIRED\"`",
			"Uint_8 int32 `parquet:\"name=Uint_8, type=INT32, convertedtype=UINT_8, repetitiontype=REQUIRED\"`",
			"Uint_16 int32 `parquet:\"name=Uint_16, type=INT32, convertedtype=UINT_16, repetitiontype=REQUIRED\"`",
			"Uint_32 int32 `parquet:\"name=Uint_32, type=INT32, convertedtype=UINT_32, repetitiontype=REQUIRED\"`",
			"Uint_64 int64 `parquet:\"name=Uint_64, type=INT64, convertedtype=UINT_64, repetitiontype=REQUIRED\"`",
			"Date int32 `parquet:\"name=Date, type=INT32, convertedtype=DATE, repetitiontype=REQUIRED\"`",
			"Date2 int32 `parquet:\"name=Date2, type=INT32, convertedtype=DATE, repetitiontype=REQUIRED\"`",
			"Timemillis int32 `parquet:\"name=Timemillis, type=INT32, convertedtype=TIME_MILLIS, repetitiontype=REQUIRED\"`",
			"Timemillis2 int32 `parquet:\"name=Timemillis2, type=INT32, repetitiontype=REQUIRED\"`",
			"Timemicros int64 `parquet:\"name=Timemicros, type=INT64, convertedtype=TIME_MICROS, repetitiontype=REQUIRED\"`",
			"Timemicros2 int64 `parquet:\"name=Timemicros2, type=INT64, repetitiontype=REQUIRED\"`",
			"Timestampmillis int64 `parquet:\"name=Timestampmillis, type=INT64, convertedtype=TIMESTAMP_MILLIS, repetitiontype=REQUIRED\"`",
			"Timestampmillis2 int64 `parquet:\"name=Timestampmillis2, type=INT64, repetitiontype=REQUIRED\"`",
			"Timestampmicros int64 `parquet:\"name=Timestampmicros, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=REQUIRED\"`",
			"Timestampmicros2 int64 `parquet:\"name=Timestampmicros2, type=INT64, repetitiontype=REQUIRED\"`",
			"Interval string `parquet:\"name=Interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, repetitiontype=REQUIRED\"`",
			"Decimal1 int32 `parquet:\"name=Decimal1, type=INT32, convertedtype=INT32, scale=2, precision=9, repetitiontype=REQUIRED\"`",
			"Decimal2 int64 `parquet:\"name=Decimal2, type=INT64, convertedtype=INT64, scale=2, precision=18, repetitiontype=REQUIRED\"`",
			"Decimal3 string `parquet:\"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=REQUIRED\"`",
			"Decimal4 string `parquet:\"name=Decimal4, type=BYTE_ARRAY, convertedtype=BYTE_ARRAY, scale=2, precision=20, repetitiontype=REQUIRED\"`",
			"Decimal5 int32 `parquet:\"name=Decimal5, type=INT32, repetitiontype=REQUIRED\"`",
			"Decimal_pointer *string `parquet:\"name=Decimal_pointer, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=OPTIONAL\"`",
			"Map map[string]int32 `parquet:\"name=Map, type=MAP, repetitiontype=REQUIRED, keytype=BYTE_ARRAY, valuetype=INT32\"`",
			"List []string `parquet:\"name=List, type=LIST, repetitiontype=REQUIRED, valuetype=BYTE_ARRAY\"`",
			"Repeated []int32 `parquet:\"name=Repeated, type=INT32, repetitiontype=REPEATED\"`",
			"NestedMap map[string]struct {",
			"Map map[string]int32 `parquet:\"name=Map, type=MAP, repetitiontype=REQUIRED, keytype=BYTE_ARRAY, valuetype=INT32\"`",
			"List []string `parquet:\"name=List, type=LIST, repetitiontype=REQUIRED, valuetype=BYTE_ARRAY\"`",
			"} `parquet:\"name=NestedMap, type=MAP, repetitiontype=REQUIRED, keytype=BYTE_ARRAY, valuetype=STRUCT\"`",
			"NestedList []struct {",
			"Map map[string]string `parquet:\"name=Map, type=MAP, repetitiontype=REQUIRED, keytype=BYTE_ARRAY, valuetype=BYTE_ARRAY\"`",
			"List []int32 `parquet:\"name=List, type=LIST, repetitiontype=REQUIRED, valuetype=INT32\"`",
			"} `parquet:\"name=NestedList, type=LIST, repetitiontype=REQUIRED, valuetype=STRUCT\"`",
			"}\n",
		}, "\n"),
		stdout)
	assert.Equal(t, "", stderr)
}
