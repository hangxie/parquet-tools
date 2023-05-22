package cmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/parquet"
)

func Test_retrieveValue_nil(t *testing.T) {
	cmd := &MetaCmd{}
	require.Nil(t, cmd.retrieveValue(nil, parquet.Type_INT32, true))
	require.Nil(t, cmd.retrieveValue(nil, parquet.Type_INT32, false))
}

func Test_retrieveValue_boolean(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0}, parquet.Type_BOOLEAN, true)
	require.Equal(t, false, s)

	s = cmd.retrieveValue([]byte{1}, parquet.Type_BOOLEAN, false)
	require.Equal(t, true, s)

	s = cmd.retrieveValue([]byte{}, parquet.Type_BOOLEAN, false)
	require.Equal(t, "failed to read data as BOOLEAN", s)
}

func Test_retrieveValue_int32(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{9, 0, 0, 0}, parquet.Type_INT32, true)
	require.Equal(t, int32(9), s)

	s = cmd.retrieveValue([]byte{251, 255, 255, 255}, parquet.Type_INT32, false)
	require.Equal(t, int32(-5), s)

	s = cmd.retrieveValue([]byte{}, parquet.Type_INT32, false)
	require.Equal(t, "failed to read data as INT32", s)
}

func Test_retrieveValue_int64(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{9, 0, 0, 0, 0, 0, 0, 0}, parquet.Type_INT64, true)
	require.Equal(t, int64(9), s)

	s = cmd.retrieveValue([]byte{251, 255, 255, 255, 255, 255, 255, 255}, parquet.Type_INT64, false)
	require.Equal(t, int64(-5), s)

	s = cmd.retrieveValue([]byte{}, parquet.Type_INT64, false)
	require.Equal(t, "failed to read data as INT64", s)
}

func Test_retrieveValue_float(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0, 0, 32, 192}, parquet.Type_FLOAT, true)
	require.Equal(t, float32(-2.5), s)

	s = cmd.retrieveValue([]byte{0, 0, 0, 64}, parquet.Type_FLOAT, false)
	require.Equal(t, float32(2), s)

	s = cmd.retrieveValue([]byte{}, parquet.Type_FLOAT, false)
	require.Equal(t, "failed to read data as FLOAT", s)
}

func Test_retrieveValue_double(t *testing.T) {
	cmd := &MetaCmd{}
	s := cmd.retrieveValue([]byte{0, 0, 0, 0, 0, 0, 0, 64}, parquet.Type_DOUBLE, true)
	require.Equal(t, float64(2), s)

	s = cmd.retrieveValue([]byte{0, 0, 0, 0, 0, 0, 4, 192}, parquet.Type_DOUBLE, false)
	require.Equal(t, float64(-2.5), s)

	s = cmd.retrieveValue([]byte{}, parquet.Type_DOUBLE, false)
	require.Equal(t, "failed to read data as DOUBLE", s)
}

func Test_retrieveValue_string(t *testing.T) {
	cmd := &MetaCmd{}
	testData := []byte("ab")
	require.Equal(t, "YWI=", cmd.retrieveValue(testData, parquet.Type_BYTE_ARRAY, true))
	require.Equal(t, "ab", cmd.retrieveValue(testData, parquet.Type_BYTE_ARRAY, false))
}

func Test_MetaCmd_Run_non_existent(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.URI = "file/does/not/exist"

	err := cmd.Run()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), string("failed to open local file"))
}

func Test_MetaCmd_Run_good_base64(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = true
	cmd.URI = "../testdata/good.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-good-base64.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	require.Nil(t, err)
	require.Equal(t, "c3RlcGhfY3Vycnk=", meta.RowGroups[0].Columns[0].MaxValue)
	require.Equal(t, "ZmlsYQ==", meta.RowGroups[0].Columns[0].MinValue)
}

func Test_MetaCmd_Run_good_raw(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/good.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-good-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	require.Nil(t, err)
	require.Equal(t, "steph_curry", meta.RowGroups[0].Columns[0].MaxValue)
	require.Equal(t, "fila", meta.RowGroups[0].Columns[0].MinValue)
}

func Test_MetaCmd_Run_good_nil_statistics(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/nil-statistics.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-nil-statistics-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	require.Nil(t, err)
	require.Nil(t, meta.RowGroups[0].Columns[1].MaxValue)
	require.Nil(t, meta.RowGroups[0].Columns[1].MinValue)
	require.Nil(t, meta.RowGroups[0].Columns[1].NullCount)
	require.Nil(t, meta.RowGroups[0].Columns[1].DistinctCount)
}

func Test_MetaCmd_Run_good_nil_int96_min_max(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/int96-nil-min-max.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/int96-nil-min-max.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	require.Nil(t, err)
	require.Nil(t, meta.RowGroups[0].Columns[1].MaxValue)
	require.Nil(t, meta.RowGroups[0].Columns[1].MinValue)
	require.NotNil(t, meta.RowGroups[0].Columns[1].NullCount)
	require.Equal(t, *meta.RowGroups[0].Columns[1].NullCount, int64(10))
}

func Test_MetaCmd_Run_good_sorting_col(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = true
	cmd.URI = "../testdata/sorting-col.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-sorting-col-base64.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)

	// double check fields we care about
	meta := parquetMeta{}
	err := json.Unmarshal([]byte(stdout), &meta)
	require.Nil(t, err)
	require.Equal(t, "DESC", *meta.RowGroups[0].Columns[0].Index)
	require.Equal(t, "ASC", *meta.RowGroups[0].Columns[1].Index)
	require.Nil(t, meta.RowGroups[0].Columns[2].Index)
}

func Test_MetaCmd_Run_good_reinterpret_scalar(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/reinterpret-scalar.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-reinterpret-scalar-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_pointer(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/reinterpret-pointer.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-reinterpret-pointer-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_list(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/reinterpret-list.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-reinterpret-list-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_map_key(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/reinterpret-map-key.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-reinterpret-map-key-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_map_value(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/reinterpret-map-value.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-reinterpret-map-value-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}

func Test_MetaCmd_Run_good_reinterpret_composite(t *testing.T) {
	cmd := &MetaCmd{}
	cmd.Base64 = false
	cmd.URI = "../testdata/reinterpret-composite.parquet"

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run())
	})
	expected := loadExpected(t, "../testdata/golden/meta-reinterpret-composite-raw.json")
	require.Equal(t, expected, stdout)
	require.Equal(t, "", stderr)
}
