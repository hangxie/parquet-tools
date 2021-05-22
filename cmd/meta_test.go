package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_retrieveValue_nil(t *testing.T) {
	cmd := &MetaCmd{}
	assert.Nil(t, cmd.retrieveValue(nil, true))
	assert.Nil(t, cmd.retrieveValue(nil, false))
}

func Test_retrieveValue_good(t *testing.T) {
	cmd := &MetaCmd{}
	testData := []byte("ab")
	assert.Equal(t, *cmd.retrieveValue(testData, true), "YWI=")
	assert.Equal(t, *cmd.retrieveValue(testData, false), "ab")
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
}
