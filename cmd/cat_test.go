package cmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CatCmd_Run_non_existent_file(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "file/does/not/exist",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open local")
}

func Test_CatCmd_Run_invalid_limit(t *testing.T) {
	cmd := &CatCmd{
		Limit:       -10,
		PageSize:    10,
		SampleRatio: 0.5,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid limit")
}

func Test_CatCmd_Run_default_limit(t *testing.T) {
	cmd := &CatCmd{
		Limit:       0,
		PageSize:    10,
		SampleRatio: 0.5,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		err := cmd.Run(&Context{})
		assert.Nil(t, err)
		assert.Equal(t, cmd.Limit, int64(1<<63-1))
	})
	assert.NotEqual(t, stdout, "")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_invalid_page_size(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    0,
		SampleRatio: 0.5,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid page size")
}

func Test_CatCmd_Run_invalid_sampling_too_big(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 2.0,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid sampling")
}

func Test_CatCmd_Run_invalid_sampling_too_small(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: -0.5,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid sampling")
}

func Test_CatCmd_Run_good_default(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"nike","Shoe_name":"air_griffey"},{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 4)
	assert.Equal(t, res[3]["Shoe_brand"], "steph_curry")
}

func Test_CatCmd_Run_good_limit(t *testing.T) {
	cmd := &CatCmd{
		Limit:       2,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"nike","Shoe_name":"air_griffey"}]`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[1]["Shoe_brand"], "nike")
}

func Test_CatCmd_Run_good_sampling(t *testing.T) {
	cmd := &CatCmd{
		Limit:       2,
		PageSize:    10,
		SampleRatio: 0.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, "[]\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_includeRowFunc_invalid_filter(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "invalid filter"
	f, err := cmd.includeRowFunc()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse filter")
	assert.Nil(t, f)
}

func Test_CatCmd_includeRowFunc_invalid_operator(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a><b"
	f, err := cmd.includeRowFunc()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid operator")
	assert.Nil(t, f)
}

func Test_CatCmd_includeRowFunc_missing_value(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a<>"
	f, err := cmd.includeRowFunc()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "missing value in filter")
	assert.Nil(t, f)
}

func Test_CatCmd_includeRowFunc_panic(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a<>b"
	f, err := cmd.includeRowFunc()
	assert.Nil(t, err)
	assert.NotNil(t, f)
	assert.Panics(t, func() { f([]byte("bad json")) })
}

func Test_CatCmd_includeRowFunc_missing_layer(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a.b.c<>b"
	f, err := cmd.includeRowFunc()
	assert.Nil(t, err)
	assert.NotNil(t, f)
	assert.False(t, f([]byte(`{"z":{}}`)))
	assert.False(t, f([]byte(`{"a":1}`)))
	assert.False(t, f([]byte(`{"a":{"z":{"c":1}}}`)))
	assert.False(t, f([]byte(`{"a":{"b":{"z":1}}}`)))

	cmd.Filter = "a.b.c==b"
	f, err = cmd.includeRowFunc()
	assert.Nil(t, err)
	assert.NotNil(t, f)
	assert.False(t, f([]byte(`{"z":{}}`)))
	assert.False(t, f([]byte(`{"a":1}`)))
	assert.False(t, f([]byte(`{"a":{"z":{"c":1}}}`)))
	assert.False(t, f([]byte(`{"a":{"b":{"z":1}}}`)))
}

func Test_CatCmd_includeRowFunc_good_equal(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a.b==11"
	f, err := cmd.includeRowFunc()
	assert.Nil(t, err)
	assert.NotNil(t, f)
	assert.False(t, f([]byte(`{"a":{"b":12}}`)))
	assert.False(t, f([]byte(`{"a":{"b":"12"}}`)))
	assert.False(t, f([]byte(`{"a":{"b":{"c":"12"}}}`)))
	assert.False(t, f([]byte(`{"a":{"b":[1,2,3]}}`)))
	assert.False(t, f([]byte(`{"a":{"b":[11]}}`)))
	assert.False(t, f([]byte(`{"a":{"b":{"c":"11"}}}`)))

	assert.True(t, f([]byte(`{"a":{"b":11}}`)))
	assert.True(t, f([]byte(`{"a":{"b":"11"}}`)))
}

func Test_CatCmd_includeRowFunc_good_not_equal(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a.b <> 11"
	f, err := cmd.includeRowFunc()
	assert.Nil(t, err)
	assert.NotNil(t, f)
	assert.True(t, f([]byte(`{"a":{"b":12}}`)))
	assert.True(t, f([]byte(`{"a":{"b":"12"}}`)))
	assert.True(t, f([]byte(`{"a":{"b":{"c":"12"}}}`)))
	assert.True(t, f([]byte(`{"a":{"b":[1,2,3]}}`)))
	assert.True(t, f([]byte(`{"a":{"b":[11]}}`)))
	assert.True(t, f([]byte(`{"a":{"b":{"c":"11"}}}`)))

	assert.False(t, f([]byte(`{"a":{"b":11}}`)))
	assert.False(t, f([]byte(`{"a":{"b":"11"}}`)))
}

func Test_CatCmd_Run_good_filter_equal(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		Filter:      "Shoe_brand == nike",
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `[{"Shoe_brand":"nike","Shoe_name":"air_griffey"}]`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	assert.Equal(t, res[0]["Shoe_brand"], "nike")
}

func Test_CatCmd_Run_good_filter_not_equal(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		Filter:      "  Shoe_brand <>  nike  ",
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 3)
	assert.Equal(t, res[1]["Shoe_brand"], "fila")
}
