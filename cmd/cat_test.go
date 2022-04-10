package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CatCmd_Run_non_existent_file(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "file/does/not/exist",
			},
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
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		err := cmd.Run(&Context{})
		assert.Nil(t, err)
		assert.Equal(t, cmd.Limit, ^uint64(0))
	})
	assert.NotEqual(t, "", stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_invalid_page_size(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    0,
		SampleRatio: 0.5,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
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
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
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
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
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
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/good.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-good-json.json")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_stream(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/good.parquet",
			},
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-good-jsonl.json")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_bad_format(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/good.parquet",
			},
		},
		Format: "random-dude",
	}

	stdout, stderr := captureStdoutStderr(func() {
		err := cmd.Run(&Context{})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown format: random-dude")
	})
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_skip(t *testing.T) {
	cmd := &CatCmd{
		Skip:        2,
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/good.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-good-json-skip-2.json")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_all_skip(t *testing.T) {
	cmd := &CatCmd{
		Skip:        12,
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/good.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, "[]\n", stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_limit(t *testing.T) {
	cmd := &CatCmd{
		Limit:       2,
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/good.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-good-json-limit-2.json")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_sampling(t *testing.T) {
	cmd := &CatCmd{
		Limit:       2,
		PageSize:    10,
		SampleRatio: 0.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/good.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, "[]\n", stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_empty(t *testing.T) {
	cmd := &CatCmd{
		Limit:       2,
		PageSize:    10,
		SampleRatio: 0.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/empty.parquet",
			},
		},
		Format: "json",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, "[]\n", stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_scalar(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/reinterpret-scalar.parquet",
			},
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-reinterpret-scalar.jsonl")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_pointer(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/reinterpret-pointer.parquet",
			},
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-reinterpret-pointer.jsonl")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_list(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/reinterpret-list.parquet",
			},
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-reinterpret-list.jsonl")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_map_key(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/reinterpret-map-key.parquet",
			},
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-reinterpret-map-key.jsonl")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_map_value(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/reinterpret-map-value.parquet",
			},
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	expected := loadExpected(t, "testdata/golden/cat-reinterpret-map-value.jsonl")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_composite(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/reinterpret-composite.parquet",
			},
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})

	expected := loadExpected(t, "testdata/golden/cat-reinterpret-composite.jsonl")
	assert.Equal(t, expected, stdout)
	assert.Equal(t, "", stderr)
}
