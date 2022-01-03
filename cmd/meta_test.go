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
	assert.Equal(t, false, s)
	s = cmd.retrieveValue([]byte{1}, parquet.Type_BOOLEAN, false)
	assert.Equal(t, true, s)
	s = cmd.retrieveValue([]byte{}, parquet.Type_BOOLEAN, false)
	assert.Equal(t, "failed to read data as BOOLEAN", s)
}

func Test_retrieveValue_int32(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{9, 0, 0, 0}, parquet.Type_INT32, true)
	assert.Equal(t, int32(9), s)
	s = cmd.retrieveValue([]byte{251, 255, 255, 255}, parquet.Type_INT32, false)
	assert.Equal(t, int32(-5), s)
	s = cmd.retrieveValue([]byte{}, parquet.Type_INT32, false)
	assert.Equal(t, "failed to read data as INT32", s)
}

func Test_retrieveValue_int64(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{9, 0, 0, 0, 0, 0, 0, 0}, parquet.Type_INT64, true)
	assert.Equal(t, int64(9), s)
	s = cmd.retrieveValue([]byte{251, 255, 255, 255, 255, 255, 255, 255}, parquet.Type_INT64, false)
	assert.Equal(t, int64(-5), s)
	s = cmd.retrieveValue([]byte{}, parquet.Type_INT64, false)
	assert.Equal(t, "failed to read data as INT64", s)
}

func Test_retrieveValue_float(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0, 0, 32, 192}, parquet.Type_FLOAT, true)
	assert.Equal(t, float32(-2.5), s)
	s = cmd.retrieveValue([]byte{0, 0, 0, 64}, parquet.Type_FLOAT, false)
	assert.Equal(t, float32(2), s)
	s = cmd.retrieveValue([]byte{}, parquet.Type_FLOAT, false)
	assert.Equal(t, "failed to read data as FLOAT", s)
}

func Test_retrieveValue_double(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0, 0, 0, 0, 0, 0, 0, 64}, parquet.Type_DOUBLE, true)
	assert.Equal(t, float64(2), s)
	s = cmd.retrieveValue([]byte{0, 0, 0, 0, 0, 0, 4, 192}, parquet.Type_DOUBLE, false)
	assert.Equal(t, float64(-2.5), s)
	s = cmd.retrieveValue([]byte{}, parquet.Type_DOUBLE, false)
	assert.Equal(t, "failed to read data as DOUBLE", s)
}

func Test_retrieveValue_string(t *testing.T) {
	cmd := &MetaCmd{}
	testData := []byte("ab")
	assert.Equal(t, "YWI=", cmd.retrieveValue(testData, parquet.Type_BYTE_ARRAY, true))
	assert.Equal(t, "ab", cmd.retrieveValue(testData, parquet.Type_BYTE_ARRAY, false))
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
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":4,"TotalByteSize":349,"Columns":[{"PathInSchema":["Shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":165,"UncompressedSize":161,"NumValues":4,"NullCount":0,"MaxValue":"c3RlcGhfY3Vycnk=","MinValue":"ZmlsYQ=="},{"PathInSchema":["Shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":192,"UncompressedSize":188,"NumValues":4,"NullCount":0,"MaxValue":"c2hvZV9uYW1l","MinValue":"YWlyX2dyaWZmZXk="}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	assert.Nil(t, err)
	assert.Equal(t, "c3RlcGhfY3Vycnk=", meta.RowGroups[0].Columns[0].MaxValue)
	assert.Equal(t, "ZmlsYQ==", meta.RowGroups[0].Columns[0].MinValue)
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
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":4,"TotalByteSize":349,"Columns":[{"PathInSchema":["Shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":165,"UncompressedSize":161,"NumValues":4,"NullCount":0,"MaxValue":"steph_curry","MinValue":"fila"},{"PathInSchema":["Shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":192,"UncompressedSize":188,"NumValues":4,"NullCount":0,"MaxValue":"shoe_name","MinValue":"air_griffey"}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	assert.Nil(t, err)
	assert.Equal(t, "steph_curry", meta.RowGroups[0].Columns[0].MaxValue)
	assert.Equal(t, "fila", meta.RowGroups[0].Columns[0].MinValue)
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
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":20,"TotalByteSize":1699,"Columns":[{"PathInSchema":["Name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN","PLAIN_DICTIONARY","RLE_DICTIONARY"],"CompressedSize":518,"UncompressedSize":639,"NumValues":20,"NullCount":0,"MaxValue":"Student Name_9","MinValue":"Student Name"},{"PathInSchema":["Age"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20},{"PathInSchema":["Id"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":346,"UncompressedSize":404,"NumValues":20},{"PathInSchema":["Weight"],"Type":"FLOAT","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20},{"PathInSchema":["Sex"],"Type":"BOOLEAN","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":144,"UncompressedSize":136,"NumValues":20}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)

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
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":20,"TotalByteSize":1699,"Columns":[{"PathInSchema":["Name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN","PLAIN_DICTIONARY","RLE_DICTIONARY"],"CompressedSize":518,"UncompressedSize":639,"NumValues":20,"NullCount":0,"MaxValue":"U3R1ZGVudCBOYW1lXzk=","MinValue":"U3R1ZGVudCBOYW1l","Index":"DESC"},{"PathInSchema":["Age"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20,"NullCount":0,"MaxValue":24,"MinValue":20,"Index":"ASC"},{"PathInSchema":["Id"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":346,"UncompressedSize":404,"NumValues":20,"NullCount":0,"MaxValue":9,"MinValue":0},{"PathInSchema":["Weight"],"Type":"FLOAT","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":266,"UncompressedSize":260,"NumValues":20,"NullCount":0,"MaxValue":50.9,"MinValue":50},{"PathInSchema":["Sex"],"Type":"BOOLEAN","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":144,"UncompressedSize":136,"NumValues":20,"NullCount":0,"MaxValue":true,"MinValue":false}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	assert.Nil(t, err)
	assert.Equal(t, "DESC", *meta.RowGroups[0].Columns[0].Index)
	assert.Equal(t, "ASC", *meta.RowGroups[0].Columns[1].Index)
	assert.Nil(t, meta.RowGroups[0].Columns[2].Index)
}

func Test_MetaCmd_Run_good_reinterpret_scalar(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-scalar.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":11,"TotalByteSize":2063,"Columns":[{"PathInSchema":["V1"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":232,"UncompressedSize":224,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V2"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":331,"UncompressedSize":332,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V3"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":399,"UncompressedSize":440,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V4"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":195,"UncompressedSize":187,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V5"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":399,"UncompressedSize":440,"NumValues":11,"NullCount":0,"MaxValue":125,"MinValue":0},{"PathInSchema":["V6"],"Type":"INT96","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":439,"UncompressedSize":440,"NumValues":11,"NullCount":0,"MaxValue":"2022-01-01T11:11:11.011011Z","MinValue":"2022-01-01T01:01:01.001001Z"}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_pointer(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-pointer.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":12,"TotalByteSize":2219,"Columns":[{"PathInSchema":["V1"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":258,"UncompressedSize":250,"NumValues":12,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V2"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":357,"UncompressedSize":358,"NumValues":12,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V3"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":424,"UncompressedSize":466,"NumValues":12,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V4"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":221,"UncompressedSize":213,"NumValues":12,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V5"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":425,"UncompressedSize":466,"NumValues":12,"NullCount":1,"MaxValue":125,"MinValue":0},{"PathInSchema":["V6"],"Type":"INT96","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":465,"UncompressedSize":466,"NumValues":12,"NullCount":1,"MaxValue":"2022-01-01T11:11:11.011011Z","MinValue":"2022-01-01T01:01:01.001001Z"}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_list(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-list.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":11,"TotalByteSize":3541,"Columns":[{"PathInSchema":["V1","List","Element"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":338,"UncompressedSize":377,"NumValues":31,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V2","List","Element"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":398,"UncompressedSize":563,"NumValues":31,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V3","List","Element"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":501,"UncompressedSize":747,"NumValues":31,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V4","List","Element"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":298,"UncompressedSize":359,"NumValues":31,"NullCount":1,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V5","List","Element"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":469,"UncompressedSize":747,"NumValues":31,"NullCount":1,"MaxValue":125,"MinValue":25},{"PathInSchema":["V6","List","Element"],"Type":"INT96","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":525,"UncompressedSize":748,"NumValues":31,"NullCount":1,"MaxValue":"2022-01-01T11:11:11.011011Z","MinValue":"2022-01-01T01:01:01.001001Z"}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_map_key(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-map-key.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":1,"TotalByteSize":3201,"Columns":[{"PathInSchema":["V1","Key_value","Key"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":105,"UncompressedSize":103,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V1","Key_value","Value"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":177,"UncompressedSize":276,"NumValues":11,"NullCount":0,"MaxValue":"INT32-[1.25]","MinValue":"INT32-[-0.25]"},{"PathInSchema":["V2","Key_value","Key"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":133,"UncompressedSize":165,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V2","Key_value","Value"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":170,"UncompressedSize":276,"NumValues":11,"NullCount":0,"MaxValue":"INT64-[1.25]","MinValue":"INT64-[-0.25]"},{"PathInSchema":["V3","Key_value","Key"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":151,"UncompressedSize":225,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V3","Key_value","Value"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":247,"UncompressedSize":501,"NumValues":11,"NullCount":0,"MaxValue":"FIXED_LEN_BYTE_ARRAY-[1.25]","MinValue":"FIXED_LEN_BYTE_ARRAY-[-0.25]"},{"PathInSchema":["V4","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":99,"UncompressedSize":104,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V4","Key_value","Value"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":202,"UncompressedSize":351,"NumValues":11,"NullCount":0,"MaxValue":"BYTE_ARRAY-[1.25]","MinValue":"BYTE_ARRAY-[-0.25]"},{"PathInSchema":["V5","Key_value","Key"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":127,"UncompressedSize":164,"NumValues":6,"NullCount":0,"MaxValue":125,"MinValue":0},{"PathInSchema":["V5","Key_value","Value"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":149,"UncompressedSize":198,"NumValues":6,"NullCount":0,"MaxValue":"INTERVAL-[75]","MinValue":"INTERVAL-[0]"},{"PathInSchema":["V6","Key_value","Key"],"Type":"INT96","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":201,"UncompressedSize":225,"NumValues":11,"NullCount":0,"MaxValue":"2022-01-01T11:11:11.011011Z","MinValue":"2022-01-01T01:01:01.001001Z"},{"PathInSchema":["V6","Key_value","Value"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":395,"UncompressedSize":613,"NumValues":11,"NullCount":0,"MaxValue":"INT96-[2022-01-01T11:11:11.011011Z]","MinValue":"INT96-[2022-01-01T01:01:01.001001Z]"}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_map_value(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-map-value.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":1,"TotalByteSize":2217,"Columns":[{"PathInSchema":["V1","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":135,"UncompressedSize":195,"NumValues":11,"NullCount":0,"MaxValue":"value-9","MinValue":"value-0"},{"PathInSchema":["V1","Key_value","Value"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":105,"UncompressedSize":103,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V2","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":136,"UncompressedSize":195,"NumValues":11,"NullCount":0,"MaxValue":"value-9","MinValue":"value-0"},{"PathInSchema":["V2","Key_value","Value"],"Type":"INT64","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":134,"UncompressedSize":165,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V3","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":133,"UncompressedSize":195,"NumValues":11,"NullCount":0,"MaxValue":"value-9","MinValue":"value-0"},{"PathInSchema":["V3","Key_value","Value"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":148,"UncompressedSize":225,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V4","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":136,"UncompressedSize":195,"NumValues":11,"NullCount":0,"MaxValue":"value-9","MinValue":"value-0"},{"PathInSchema":["V4","Key_value","Value"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":100,"UncompressedSize":104,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["V5","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":134,"UncompressedSize":195,"NumValues":11,"NullCount":0,"MaxValue":"value-9","MinValue":"value-0"},{"PathInSchema":["V5","Key_value","Value"],"Type":"FIXED_LEN_BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":145,"UncompressedSize":225,"NumValues":11,"NullCount":0,"MaxValue":125,"MinValue":0},{"PathInSchema":["V6","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":135,"UncompressedSize":195,"NumValues":11,"NullCount":0,"MaxValue":"value-9","MinValue":"value-0"},{"PathInSchema":["V6","Key_value","Value"],"Type":"INT96","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":201,"UncompressedSize":225,"NumValues":11,"NullCount":0,"MaxValue":"2022-01-01T11:11:11.011011Z","MinValue":"2022-01-01T01:01:01.001001Z"}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_composite(t *testing.T) {
	cmd := &MetaCmd{
		Base64: false,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-composite.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"NumRowGroups":1,"RowGroups":[{"NumRows":1,"TotalByteSize":2129,"Columns":[{"PathInSchema":["Map","Key_value","Key"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":105,"UncompressedSize":103,"NumValues":11,"NullCount":0,"MaxValue":1.25,"MinValue":-1.25},{"PathInSchema":["Map","Key_value","Value","List","Element","EmbeddedMap","Key_value","Key"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":232,"UncompressedSize":558,"NumValues":71,"NullCount":1,"MaxValue":40.04,"MinValue":0},{"PathInSchema":["Map","Key_value","Value","List","Element","EmbeddedMap","Key_value","Value"],"Type":"INT32","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":181,"UncompressedSize":438,"NumValues":71,"NullCount":1,"MaxValue":0.04,"MinValue":0},{"PathInSchema":["Map","Key_value","Value","List","Element","EmbeddedList","List","Element"],"Type":"INT96","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":788,"UncompressedSize":1030,"NumValues":71,"NullCount":1,"MaxValue":"2022-01-03T23:11:10.07007Z","MinValue":"2022-01-01T01:01:01.001001Z"}]}]}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}
