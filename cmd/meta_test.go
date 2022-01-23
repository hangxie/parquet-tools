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
	expected := loadExpected(t, "testdata/golden/meta-good-base64.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-good-raw.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-nil-statistics-raw.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-sorting-col-base64.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-reinterpret-scalar-raw.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-reinterpret-pointer-raw.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-reinterpret-list-raw.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-reinterpret-map-key-raw.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-reinterpret-map-value-raw.json")
	assert.Equal(t, expected, stdout)
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
	expected := loadExpected(t, "testdata/golden/meta-reinterpret-composite-raw.json")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}
