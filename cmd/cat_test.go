package cmd

import (
	"encoding/json"
	"strings"
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
		Format: "json",
	}

	err := cmd.Run(&Context{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to open local")
}

func Test_CatCmd_Run_default_limit(t *testing.T) {
	cmd := &CatCmd{
		Limit:       0,
		PageSize:    10,
		SampleRatio: 0.5,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		err := cmd.Run(&Context{})
		assert.Nil(t, err)
		assert.Equal(t, cmd.Limit, ^uint64(0))
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
		Format: "json",
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
		Format: "json",
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
		Format: "json",
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
		Format: "json",
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

func Test_CatCmd_Run_good_stream(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout,
		strings.Join([]string{
			`{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"}`,
			`{"Shoe_brand":"nike","Shoe_name":"air_griffey"}`,
			`{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"}`,
			`{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}`,
			"",
		},
			"\n"))
	assert.Equal(t, stderr, "")

	// double check
	items := strings.Split(stdout, "\n")
	assert.Equal(t, len(items), 5)

	res := map[string]string{}
	err := json.Unmarshal([]byte(items[3]), &res)
	assert.Nil(t, err)
	assert.Equal(t, res["Shoe_brand"], "steph_curry")
}

func Test_CatCmd_Run_bad_format(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
		Format: "random-dude",
	}

	stdout, stderr := captureStdoutStderr(func() {
		err := cmd.Run(&Context{})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown format: random-dude")
	})
	assert.Equal(t, stdout, "")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_skip(t *testing.T) {
	cmd := &CatCmd{
		Skip:        2,
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `[{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[1]["Shoe_brand"], "steph_curry")
}

func Test_CatCmd_Run_good_all_skip(t *testing.T) {
	cmd := &CatCmd{
		Skip:        12,
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `[]`+
		"\n")
	assert.Equal(t, stderr, "")

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 0)
}

func Test_CatCmd_Run_good_limit(t *testing.T) {
	cmd := &CatCmd{
		Limit:       2,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/good.parquet",
		},
		Format: "json",
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
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, "[]\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_zero(t *testing.T) {
	cmd := &CatCmd{
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/decimals.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Decimal1":0,"Decimal2":0,"Decimal3":0,"Decimal4":0,"Decimal_pointer":null}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_fraction(t *testing.T) {
	cmd := &CatCmd{
		Skip:        1,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/decimals.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Decimal1":0.11,"Decimal2":0.11,"Decimal3":0.11,"Decimal4":0.11,"Decimal_pointer":0.11}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_normal(t *testing.T) {
	cmd := &CatCmd{
		Skip:        2,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/decimals.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Decimal1":2.22,"Decimal2":2.22,"Decimal3":2.22,"Decimal4":2.22,"Decimal_pointer":2.22}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_negative_zero(t *testing.T) {
	cmd := &CatCmd{
		Skip:        3,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/decimals.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Decimal1":0,"Decimal2":0,"Decimal3":0,"Decimal4":0,"Decimal_pointer":null}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_negative_fraction(t *testing.T) {
	cmd := &CatCmd{
		Skip:        4,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/decimals.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Decimal1":-0.11,"Decimal2":-0.11,"Decimal3":-0.11,"Decimal4":-0.11,"Decimal_pointer":-0.11}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_negative_normal(t *testing.T) {
	cmd := &CatCmd{
		Skip:        5,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/decimals.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Decimal1":-2.22,"Decimal2":-2.22,"Decimal3":-2.22,"Decimal4":-2.22,"Decimal_pointer":-2.22}`+"\n")
	assert.Equal(t, stderr, "")
}
