package cmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
)

func Test_retrieveValue_nil(t *testing.T) {
	cmd := &MetaCmd{}
	assert.Nil(t, cmd.retrieveValue(nil, parquet.Type_INT32, true))
	assert.Nil(t, cmd.retrieveValue(nil, parquet.Type_INT32, false))
}

func Test_retrieveValue_boolean(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0}, parquet.Type_BOOLEAN, true)
	assert.Equal(t, s, false)
	s = cmd.retrieveValue([]byte{1}, parquet.Type_BOOLEAN, false)
	assert.Equal(t, s, true)
	s = cmd.retrieveValue([]byte{}, parquet.Type_BOOLEAN, false)
	assert.Equal(t, s, "failed to read data as BOOLEAN")
}

func Test_retrieveValue_int32(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{9, 0, 0, 0}, parquet.Type_INT32, true)
	assert.Equal(t, s, int32(9))
	s = cmd.retrieveValue([]byte{251, 255, 255, 255}, parquet.Type_INT32, false)
	assert.Equal(t, s, int32(-5))
	s = cmd.retrieveValue([]byte{}, parquet.Type_INT32, false)
	assert.Equal(t, s, "failed to read data as INT32")
}

func Test_retrieveValue_int64(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{9, 0, 0, 0, 0, 0, 0, 0}, parquet.Type_INT64, true)
	assert.Equal(t, s, int64(9))
	s = cmd.retrieveValue([]byte{251, 255, 255, 255, 255, 255, 255, 255}, parquet.Type_INT64, false)
	assert.Equal(t, s, int64(-5))
	s = cmd.retrieveValue([]byte{}, parquet.Type_INT64, false)
	assert.Equal(t, s, "failed to read data as INT64")
}

func Test_retrieveValue_float(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0, 0, 32, 192}, parquet.Type_FLOAT, true)
	assert.Equal(t, s, float32(-2.5))
	s = cmd.retrieveValue([]byte{0, 0, 0, 64}, parquet.Type_FLOAT, false)
	assert.Equal(t, s, float32(2))
	s = cmd.retrieveValue([]byte{}, parquet.Type_FLOAT, false)
	assert.Equal(t, s, "failed to read data as FLOAT")
}

func Test_retrieveValue_double(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0, 0, 0, 0, 0, 0, 0, 64}, parquet.Type_DOUBLE, true)
	assert.Equal(t, s, float64(2))
	s = cmd.retrieveValue([]byte{0, 0, 0, 0, 0, 0, 4, 192}, parquet.Type_DOUBLE, false)
	assert.Equal(t, s, float64(-2.5))
	s = cmd.retrieveValue([]byte{}, parquet.Type_DOUBLE, false)
	assert.Equal(t, s, "failed to read data as DOUBLE")
}

func Test_retrieveValue_string(t *testing.T) {
	cmd := &MetaCmd{}
	testData := []byte("ab")
	assert.Equal(t, cmd.retrieveValue(testData, parquet.Type_BYTE_ARRAY, true), "YWI=")
	assert.Equal(t, cmd.retrieveValue(testData, parquet.Type_BYTE_ARRAY, false), "ab")
}

func Test_MetaCmd_Run_non_existent(t *testing.T) {
	cmd := &MetaCmd{
		CommonOption: CommonOption{
			URI: "file/does/not/exist",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), string("failed to open local file"))
}

func Test_MetaCmd_Run_good_base64(t *testing.T) {
	cmd := &MetaCmd{
		Base64: true,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"NumRowGroups":1,"RowGroups":[{"NumRows":4,"TotalByteSize":349,"Columns":[{"PathInSchema":["Shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":165,"UncompressedSize":161,"NumValues":4,"NullCount":0,"MaxValue":"c3RlcGhfY3Vycnk=","MinValue":"ZmlsYQ=="},{"PathInSchema":["Shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":192,"UncompressedSize":188,"NumValues":4,"NullCount":0,"MaxValue":"c2hvZV9uYW1l","MinValue":"YWlyX2dyaWZmZXk="}]}]}`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	assert.Nil(t, err)
	assert.Equal(t, meta.RowGroups[0].Columns[0].MaxValue, "c3RlcGhfY3Vycnk=")
	assert.Equal(t, meta.RowGroups[0].Columns[0].MinValue, "ZmlsYQ==")
}

func Test_MetaCmd_Run_good_raw(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"NumRowGroups":1,"RowGroups":[{"NumRows":4,"TotalByteSize":349,"Columns":[{"PathInSchema":["Shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":165,"UncompressedSize":161,"NumValues":4,"NullCount":0,"MaxValue":"steph_curry","MinValue":"fila"},{"PathInSchema":["Shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":192,"UncompressedSize":188,"NumValues":4,"NullCount":0,"MaxValue":"shoe_name","MinValue":"air_griffey"}]}]}`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	assert.Nil(t, err)
	assert.Equal(t, meta.RowGroups[0].Columns[0].MaxValue, "steph_curry")
	assert.Equal(t, meta.RowGroups[0].Columns[0].MinValue, "fila")
}

func Test_MetaCmd_Run_good_nil_statistics(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/nil-statistics.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"NumRowGroups":1,"RowGroups":[{"NumRows":20,"TotalByteSize":1699,"Columns":[{"PathInSchema":["Name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN","PLAIN_DICTIONARY","RLE_DICTIONARY"],"CompressedSize":518,"UncompressedSize":639,"NumValues":20,"NullCount":0,"MaxValue":"Student Name_9","MinValue":"Student Name"},{"PathInSchema":["Age"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20},{"PathInSchema":["Id"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":346,"UncompressedSize":404,"NumValues":20},{"PathInSchema":["Weight"],"Type":"FLOAT","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20},{"PathInSchema":["Sex"],"Type":"BOOLEAN","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":144,"UncompressedSize":136,"NumValues":20}]}]}`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	assert.Nil(t, err)
	assert.Nil(t, meta.RowGroups[0].Columns[1].MaxValue)
	assert.Nil(t, meta.RowGroups[0].Columns[1].MinValue)
	assert.Nil(t, meta.RowGroups[0].Columns[1].NullCount)
	assert.Nil(t, meta.RowGroups[0].Columns[1].DistinctCount)
}

func Test_MetaCmd_Run_good_sorting_col(t *testing.T) {
	cmd := &MetaCmd{
		Base64: true,
		CommonOption: CommonOption{
			URI: "testdata/sorting-col.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"NumRowGroups":1,"RowGroups":[{"NumRows":20,"TotalByteSize":1699,"Columns":[{"PathInSchema":["Name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN","PLAIN_DICTIONARY","RLE_DICTIONARY"],"CompressedSize":518,"UncompressedSize":639,"NumValues":20,"NullCount":0,"MaxValue":"U3R1ZGVudCBOYW1lXzk=","MinValue":"U3R1ZGVudCBOYW1l","Index":"DESC"},{"PathInSchema":["Age"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20,"NullCount":0,"MaxValue":24,"MinValue":20,"Index":"ASC"},{"PathInSchema":["Id"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":346,"UncompressedSize":404,"NumValues":20,"NullCount":0,"MaxValue":9,"MinValue":0},{"PathInSchema":["Weight"],"Type":"FLOAT","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20,"NullCount":0,"MaxValue":50.9,"MinValue":50},{"PathInSchema":["Sex"],"Type":"BOOLEAN","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":144,"UncompressedSize":136,"NumValues":20,"NullCount":0,"MaxValue":true,"MinValue":false}]}]}`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	assert.Nil(t, err)
	assert.Equal(t, *meta.RowGroups[0].Columns[0].Index, "DESC")
	assert.Equal(t, *meta.RowGroups[0].Columns[1].Index, "ASC")
	assert.Nil(t, meta.RowGroups[0].Columns[2].Index)
}

func Test_MetaCmd_Run_good_decimal(t *testing.T) {
	cmd := &MetaCmd{
		Base64: true,
		CommonOption: CommonOption{
			URI: "testdata/decimals.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"NumRowGroups":1,"RowGroups":[{"NumRows":6,"TotalByteSize":1129,"Columns":[{"PathInSchema":["Decimal1"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":165,"UncompressedSize":159,"NumValues":6,"NullCount":0,"MaxValue":2.22,"MinValue":-2.22},{"PathInSchema":["Decimal2"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":237,"UncompressedSize":231,"NumValues":6,"NullCount":0,"MaxValue":2.22,"MinValue":-2.22},{"PathInSchema":["Decimal3"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":277,"UncompressedSize":303,"NumValues":6,"NullCount":0,"MaxValue":2.22,"MinValue":-2.22},{"PathInSchema":["Decimal4"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":141,"UncompressedSize":135,"NumValues":6,"NullCount":0,"MaxValue":2.22,"MinValue":-2.22},{"PathInSchema":["Decimal_pointer"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":300,"UncompressedSize":301,"NumValues":6,"NullCount":1,"MaxValue":2.22,"MinValue":-2.22}]}]}`+
		"\n")
	assert.Equal(t, stderr, "")
}
