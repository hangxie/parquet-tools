package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
)

func Test_SchemaCmd_typeStr_both_nil(t *testing.T) {
	assert.Equal(t, typeStr(parquet.SchemaElement{Type: nil, ConvertedType: nil}), "")
}

func Test_SchemaCmd_typeStr_converted_type_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	assert.Equal(t, typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: nil}), "FIXED_LEN_BYTE_ARRAY")
}

func Test_SchemaCmd_typeStr_type_nil(t *testing.T) {
	cType := parquet.ConvertedType_LIST
	assert.Equal(t, typeStr(parquet.SchemaElement{Type: nil, ConvertedType: &cType}), "LIST")
}
func Test_SchemaCmd_typeStr_both_non_nil(t *testing.T) {
	pType := parquet.Type_FIXED_LEN_BYTE_ARRAY
	cType := parquet.ConvertedType_LIST
	assert.Equal(t, typeStr(parquet.SchemaElement{Type: &pType, ConvertedType: &cType}), "FIXED_LEN_BYTE_ARRAY")
}

func Test_SchemaCmd_Run_non_existent(t *testing.T) {
	cmd := &SchemaCmd{
		CommonOption: CommonOption{
			URI: "file/does/not/exist",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), string("failed to open local file"))
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
	assert.Equal(t, stdout, `{"repetition_type":"REQUIRED","name":"Parquet_go_root","num_children":36,"Children":[{"type":"BOOLEAN","type_length":0,"repetition_type":"REQUIRED","name":"Bool","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int32","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Int64","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"INT96","type_length":0,"repetition_type":"REQUIRED","name":"Int96","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"FLOAT","type_length":0,"repetition_type":"REQUIRED","name":"Float","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"DOUBLE","type_length":0,"repetition_type":"REQUIRED","name":"Double","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Bytearray","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"FIXED_LEN_BYTE_ARRAY","type_length":10,"repetition_type":"REQUIRED","name":"FixedLenByteArray","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Utf8","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int_8","converted_type":"INT_8","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":8,"isSigned":true}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int_16","converted_type":"INT_16","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":16,"isSigned":true}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Int_32","converted_type":"INT_32","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":32,"isSigned":true}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Int_64","converted_type":"INT_64","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":64,"isSigned":true}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Uint_8","converted_type":"UINT_8","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":8,"isSigned":false}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Uint_16","converted_type":"UINT_16","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":16,"isSigned":false}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Uint_32","converted_type":"UINT_32","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":32,"isSigned":false}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Uint_64","converted_type":"UINT_64","scale":0,"precision":0,"field_id":0,"logicalType":{"INTEGER":{"bitWidth":64,"isSigned":false}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Date","converted_type":"DATE","scale":0,"precision":0,"field_id":0,"logicalType":{"DATE":{}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Date2","converted_type":"DATE","scale":0,"precision":0,"field_id":0,"logicalType":{"DATE":{}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Timemillis","converted_type":"TIME_MILLIS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":false,"unit":{"MILLIS":{}}}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Timemillis2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":true,"unit":{"MILLIS":{}}}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timemicros","converted_type":"TIME_MICROS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timemicros2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIME":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmillis","converted_type":"TIMESTAMP_MILLIS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":false,"unit":{"MILLIS":{}}}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmillis2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":true,"unit":{"MILLIS":{}}}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmicros","converted_type":"TIMESTAMP_MICROS","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Timestampmicros2","scale":0,"precision":0,"field_id":0,"logicalType":{"TIMESTAMP":{"isAdjustedToUTC":false,"unit":{"MICROS":{}}}},"Children":null},{"type":"FIXED_LEN_BYTE_ARRAY","type_length":12,"repetition_type":"REQUIRED","name":"Interval","converted_type":"INTERVAL","scale":0,"precision":0,"field_id":0,"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Decimal1","converted_type":"DECIMAL","scale":2,"precision":9,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":9}},"Children":null},{"type":"INT64","type_length":0,"repetition_type":"REQUIRED","name":"Decimal2","converted_type":"DECIMAL","scale":2,"precision":18,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":18}},"Children":null},{"type":"FIXED_LEN_BYTE_ARRAY","type_length":12,"repetition_type":"REQUIRED","name":"Decimal3","converted_type":"DECIMAL","scale":2,"precision":10,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":10}},"Children":null},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Decimal4","converted_type":"DECIMAL","scale":2,"precision":20,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":20}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Decimal5","scale":2,"precision":9,"field_id":0,"logicalType":{"DECIMAL":{"scale":2,"precision":9}},"Children":null},{"repetition_type":"REQUIRED","name":"Map","num_children":1,"converted_type":"MAP","Children":[{"repetition_type":"REPEATED","name":"Key_value","num_children":2,"converted_type":"MAP_KEY_VALUE","Children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Key","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}},"Children":null},{"type":"INT32","type_length":0,"repetition_type":"REQUIRED","name":"Value","scale":0,"precision":0,"field_id":0,"Children":null}]}]},{"repetition_type":"REQUIRED","name":"List","num_children":1,"converted_type":"LIST","Children":[{"repetition_type":"REPEATED","name":"List","num_children":1,"Children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Element","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}},"Children":null}]}]},{"type":"INT32","type_length":0,"repetition_type":"REPEATED","name":"Repeated","scale":0,"precision":0,"field_id":0,"Children":null}]}`+
		"\n")
	assert.Equal(t, stderr, "")
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
	assert.Equal(t, stdout, `{"Tag":"name=Parquet_go_root, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Bool, type=BOOLEAN, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Int32, type=INT32, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Int64, type=INT64, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Int96, type=INT96, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Float, type=FLOAT, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Double, type=DOUBLE, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Bytearray, type=BYTE_ARRAY, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Utf8, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Int_8, type=INT32, convertedtype=INT_8, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Int_16, type=INT32, convertedtype=INT_16, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Int_32, type=INT32, convertedtype=INT_32, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Int_64, type=INT64, convertedtype=INT_64, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Uint_8, type=INT32, convertedtype=UINT_8, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Uint_16, type=INT32, convertedtype=UINT_16, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Uint_32, type=INT32, convertedtype=UINT_32, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Uint_64, type=INT64, convertedtype=UINT_64, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Date, type=INT32, convertedtype=DATE, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Date2, type=INT32, convertedtype=DATE, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timemillis, type=INT32, convertedtype=TIME_MILLIS, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timemillis2, type=INT32, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timemicros, type=INT64, convertedtype=TIME_MICROS, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timemicros2, type=INT64, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timestampmillis, type=INT64, convertedtype=TIMESTAMP_MILLIS, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timestampmillis2, type=INT64, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timestampmicros, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Timestampmicros2, type=INT64, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Decimal5, type=INT32, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Map, type=MAP, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Key, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED","Fields":null},{"Tag":"name=Value, type=INT32, repetitiontype=REQUIRED","Fields":null}]},{"Tag":"name=List, type=LIST, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Element, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED","Fields":null}]},{"Tag":"name=Repeated, type=INT32, repetitiontype=REPEATED","Fields":null}]}`+
		"\n")
	assert.Equal(t, stderr, "")
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

	assert.Equal(t, stdout, "To be implemented.\n")
	assert.Equal(t, stderr, "")
}
