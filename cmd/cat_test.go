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
	assert.NotEqual(t, "", stdout)
	assert.Equal(t, "", stderr)
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
	assert.Equal(t, `[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"nike","Shoe_name":"air_griffey"},{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]`+"\n", stdout)
	assert.Equal(t, "", stderr)

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(res))
	assert.Equal(t, "steph_curry", res[3]["Shoe_brand"])
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
	assert.Equal(t,
		strings.Join([]string{
			`{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"}`,
			`{"Shoe_brand":"nike","Shoe_name":"air_griffey"}`,
			`{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"}`,
			`{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}`,
			"",
		}, "\n"),
		stdout)
	assert.Equal(t, "", stderr)

	// double check
	items := strings.Split(stdout, "\n")
	assert.Equal(t, 5, len(items))

	res := map[string]string{}
	err := json.Unmarshal([]byte(items[3]), &res)
	assert.Nil(t, err)
	assert.Equal(t, "steph_curry", res["Shoe_brand"])
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
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
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
	assert.Equal(t,
		`[{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]`+"\n",
		stdout)
	assert.Equal(t, "", stderr)

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, "steph_curry", res[1]["Shoe_brand"])
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
	assert.Equal(t, "[]\n", stdout)
	assert.Equal(t, "", stderr)

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))
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
	assert.Equal(t,
		`[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"nike","Shoe_name":"air_griffey"}]`+"\n",
		stdout)
	assert.Equal(t, "", stderr)

	// double check
	res := []map[string]string{}
	err := json.Unmarshal([]byte(stdout), &res)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, "nike", res[1]["Shoe_brand"])
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
	assert.Equal(t, "[]\n", stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_empty(t *testing.T) {
	cmd := &CatCmd{
		Limit:       2,
		PageSize:    10,
		SampleRatio: 0.0,
		CommonOption: CommonOption{
			URI: "testdata/empty.parquet",
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
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-scalar.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		strings.Join([]string{
			`{"V1":-1.25,"V2":-1.25,"V3":-1.25,"V4":-1.25,"V5":125,"V6":"2022-01-01T01:01:01.001001Z"}`,
			`{"V1":-1,"V2":-1,"V3":-1,"V4":-1,"V5":100,"V6":"2022-01-01T02:02:02.002002Z"}`,
			`{"V1":-0.75,"V2":-0.75,"V3":-0.75,"V4":-0.75,"V5":75,"V6":"2022-01-01T03:03:03.003003Z"}`,
			`{"V1":-0.5,"V2":-0.5,"V3":-0.5,"V4":-0.5,"V5":50,"V6":"2022-01-01T04:04:04.004004Z"}`,
			`{"V1":-0.25,"V2":-0.25,"V3":-0.25,"V4":-0.25,"V5":25,"V6":"2022-01-01T05:05:05.005005Z"}`,
			`{"V1":0,"V2":0,"V3":0,"V4":0,"V5":0,"V6":"2022-01-01T06:06:06.006006Z"}`,
			`{"V1":0.25,"V2":0.25,"V3":0.25,"V4":0.25,"V5":25,"V6":"2022-01-01T07:07:07.007007Z"}`,
			`{"V1":0.5,"V2":0.5,"V3":0.5,"V4":0.5,"V5":50,"V6":"2022-01-01T08:08:08.008008Z"}`,
			`{"V1":0.75,"V2":0.75,"V3":0.75,"V4":0.75,"V5":75,"V6":"2022-01-01T09:09:09.009009Z"}`,
			`{"V1":1,"V2":1,"V3":1,"V4":1,"V5":100,"V6":"2022-01-01T10:10:10.01001Z"}`,
			`{"V1":1.25,"V2":1.25,"V3":1.25,"V4":1.25,"V5":125,"V6":"2022-01-01T11:11:11.011011Z"}`,
			``,
		}, "\n"),
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_pointer(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-pointer.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		strings.Join([]string{
			`{"V1":-1.25,"V2":-1.25,"V3":-1.25,"V4":-1.25,"V5":125,"V6":"2022-01-01T01:01:01.001001Z"}`,
			`{"V1":-1,"V2":-1,"V3":-1,"V4":-1,"V5":100,"V6":"2022-01-01T02:02:02.002002Z"}`,
			`{"V1":-0.75,"V2":-0.75,"V3":-0.75,"V4":-0.75,"V5":75,"V6":"2022-01-01T03:03:03.003003Z"}`,
			`{"V1":-0.5,"V2":-0.5,"V3":-0.5,"V4":-0.5,"V5":50,"V6":"2022-01-01T04:04:04.004004Z"}`,
			`{"V1":-0.25,"V2":-0.25,"V3":-0.25,"V4":-0.25,"V5":25,"V6":"2022-01-01T05:05:05.005005Z"}`,
			`{"V1":0,"V2":0,"V3":0,"V4":0,"V5":0,"V6":"2022-01-01T06:06:06.006006Z"}`,
			`{"V1":0.25,"V2":0.25,"V3":0.25,"V4":0.25,"V5":25,"V6":"2022-01-01T07:07:07.007007Z"}`,
			`{"V1":0.5,"V2":0.5,"V3":0.5,"V4":0.5,"V5":50,"V6":"2022-01-01T08:08:08.008008Z"}`,
			`{"V1":0.75,"V2":0.75,"V3":0.75,"V4":0.75,"V5":75,"V6":"2022-01-01T09:09:09.009009Z"}`,
			`{"V1":1,"V2":1,"V3":1,"V4":1,"V5":100,"V6":"2022-01-01T10:10:10.01001Z"}`,
			`{"V1":1.25,"V2":1.25,"V3":1.25,"V4":1.25,"V5":125,"V6":"2022-01-01T11:11:11.011011Z"}`,
			`{"V1":null,"V2":null,"V3":null,"V4":null,"V5":null,"V6":null}`,
			``,
		}, "\n"),
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_list(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-list.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		strings.Join([]string{
			`{"V1":[-1.25,-1.25,-1.25,-1.25,-1.25],"V2":[-1.25,-1.25,-1.25,-1.25,-1.25],"V3":[-1.25,-1.25,-1.25,-1.25,-1.25],"V4":[-1.25,-1.25,-1.25,-1.25,-1.25],"V5":[125,125,125,125,125],"V6":["2022-01-01T01:01:01.001001Z","2022-01-01T01:01:01.001001Z","2022-01-01T01:01:01.001001Z","2022-01-01T01:01:01.001001Z","2022-01-01T01:01:01.001001Z"]}`,
			`{"V1":[-1,-1,-1,-1],"V2":[-1,-1,-1,-1],"V3":[-1,-1,-1,-1],"V4":[-1,-1,-1,-1],"V5":[100,100,100,100],"V6":["2022-01-01T02:02:02.002002Z","2022-01-01T02:02:02.002002Z","2022-01-01T02:02:02.002002Z","2022-01-01T02:02:02.002002Z"]}`,
			`{"V1":[-0.75,-0.75,-0.75],"V2":[-0.75,-0.75,-0.75],"V3":[-0.75,-0.75,-0.75],"V4":[-0.75,-0.75,-0.75],"V5":[75,75,75],"V6":["2022-01-01T03:03:03.003003Z","2022-01-01T03:03:03.003003Z","2022-01-01T03:03:03.003003Z"]}`,
			`{"V1":[-0.5,-0.5],"V2":[-0.5,-0.5],"V3":[-0.5,-0.5],"V4":[-0.5,-0.5],"V5":[50,50],"V6":["2022-01-01T04:04:04.004004Z","2022-01-01T04:04:04.004004Z"]}`,
			`{"V1":[-0.25],"V2":[-0.25],"V3":[-0.25],"V4":[-0.25],"V5":[25],"V6":["2022-01-01T05:05:05.005005Z"]}`,
			`{"V1":[],"V2":[],"V3":[],"V4":[],"V5":[],"V6":[]}`,
			`{"V1":[0.25],"V2":[0.25],"V3":[0.25],"V4":[0.25],"V5":[25],"V6":["2022-01-01T07:07:07.007007Z"]}`,
			`{"V1":[0.5,0.5],"V2":[0.5,0.5],"V3":[0.5,0.5],"V4":[0.5,0.5],"V5":[50,50],"V6":["2022-01-01T08:08:08.008008Z","2022-01-01T08:08:08.008008Z"]}`,
			`{"V1":[0.75,0.75,0.75],"V2":[0.75,0.75,0.75],"V3":[0.75,0.75,0.75],"V4":[0.75,0.75,0.75],"V5":[75,75,75],"V6":["2022-01-01T09:09:09.009009Z","2022-01-01T09:09:09.009009Z","2022-01-01T09:09:09.009009Z"]}`,
			`{"V1":[1,1,1,1],"V2":[1,1,1,1],"V3":[1,1,1,1],"V4":[1,1,1,1],"V5":[100,100,100,100],"V6":["2022-01-01T10:10:10.01001Z","2022-01-01T10:10:10.01001Z","2022-01-01T10:10:10.01001Z","2022-01-01T10:10:10.01001Z"]}`,
			`{"V1":[1.25,1.25,1.25,1.25,1.25],"V2":[1.25,1.25,1.25,1.25,1.25],"V3":[1.25,1.25,1.25,1.25,1.25],"V4":[1.25,1.25,1.25,1.25,1.25],"V5":[125,125,125,125,125],"V6":["2022-01-01T11:11:11.011011Z","2022-01-01T11:11:11.011011Z","2022-01-01T11:11:11.011011Z","2022-01-01T11:11:11.011011Z","2022-01-01T11:11:11.011011Z"]}`,
			``,
		}, "\n"),
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_map_key(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-map-key.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":{"-0.25":"INT32-[-0.25]","-0.50":"INT32-[-0.50]","-0.75":"INT32-[-0.75]","-1.00":"INT32-[-1.00]","-1.25":"INT32-[-1.25]","0.00":"INT32-[0.00]","0.25":"INT32-[0.25]","0.50":"INT32-[0.50]","0.75":"INT32-[0.75]","1.00":"INT32-[1.00]","1.25":"INT32-[1.25]"},"V2":{"-0.25":"INT64-[-0.25]","-0.50":"INT64-[-0.50]","-0.75":"INT64-[-0.75]","-1.00":"INT64-[-1.00]","-1.25":"INT64-[-1.25]","0.00":"INT64-[0.00]","0.25":"INT64-[0.25]","0.50":"INT64-[0.50]","0.75":"INT64-[0.75]","1.00":"INT64-[1.00]","1.25":"INT64-[1.25]"},"V3":{"-0.25":"FIXED_LEN_BYTE_ARRAY-[-0.25]","-0.50":"FIXED_LEN_BYTE_ARRAY-[-0.50]","-0.75":"FIXED_LEN_BYTE_ARRAY-[-0.75]","-1.00":"FIXED_LEN_BYTE_ARRAY-[-1.00]","-1.25":"FIXED_LEN_BYTE_ARRAY-[-1.25]","0.00":"FIXED_LEN_BYTE_ARRAY-[0.00]","0.25":"FIXED_LEN_BYTE_ARRAY-[0.25]","0.50":"FIXED_LEN_BYTE_ARRAY-[0.50]","0.75":"FIXED_LEN_BYTE_ARRAY-[0.75]","1.00":"FIXED_LEN_BYTE_ARRAY-[1.00]","1.25":"FIXED_LEN_BYTE_ARRAY-[1.25]"},"V4":{"-0.25":"BYTE_ARRAY-[-0.25]","-0.50":"BYTE_ARRAY-[-0.50]","-0.75":"BYTE_ARRAY-[-0.75]","-1.00":"BYTE_ARRAY-[-1.00]","-1.25":"BYTE_ARRAY-[-1.25]","0.00":"BYTE_ARRAY-[0.00]","0.25":"BYTE_ARRAY-[0.25]","0.50":"BYTE_ARRAY-[0.50]","0.75":"BYTE_ARRAY-[0.75]","1.00":"BYTE_ARRAY-[1.00]","1.25":"BYTE_ARRAY-[1.25]"},"V5":{"0":"INTERVAL-[0]","100":"INTERVAL-[100]","125":"INTERVAL-[125]","25":"INTERVAL-[25]","50":"INTERVAL-[50]","75":"INTERVAL-[75]"},"V6":{"2022-01-01T01:01:01.001001Z":"INT96-[2022-01-01T01:01:01.001001Z]","2022-01-01T02:02:02.002002Z":"INT96-[2022-01-01T02:02:02.002002Z]","2022-01-01T03:03:03.003003Z":"INT96-[2022-01-01T03:03:03.003003Z]","2022-01-01T04:04:04.004004Z":"INT96-[2022-01-01T04:04:04.004004Z]","2022-01-01T05:05:05.005005Z":"INT96-[2022-01-01T05:05:05.005005Z]","2022-01-01T06:06:06.006006Z":"INT96-[2022-01-01T06:06:06.006006Z]","2022-01-01T07:07:07.007007Z":"INT96-[2022-01-01T07:07:07.007007Z]","2022-01-01T08:08:08.008008Z":"INT96-[2022-01-01T08:08:08.008008Z]","2022-01-01T09:09:09.009009Z":"INT96-[2022-01-01T09:09:09.009009Z]","2022-01-01T10:10:10.01001Z":"INT96-[2022-01-01T10:10:10.01001Z]","2022-01-01T11:11:11.011011Z":"INT96-[2022-01-01T11:11:11.011011Z]"}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_map_value(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-map-value.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":{"value-0":-1.25,"value-1":-1,"value-10":1.25,"value-2":-0.75,"value-3":-0.5,"value-4":-0.25,"value-5":0,"value-6":0.25,"value-7":0.5,"value-8":0.75,"value-9":1},"V2":{"value-0":-1.25,"value-1":-1,"value-10":1.25,"value-2":-0.75,"value-3":-0.5,"value-4":-0.25,"value-5":0,"value-6":0.25,"value-7":0.5,"value-8":0.75,"value-9":1},"V3":{"value-0":-1.25,"value-1":-1,"value-10":1.25,"value-2":-0.75,"value-3":-0.5,"value-4":-0.25,"value-5":0,"value-6":0.25,"value-7":0.5,"value-8":0.75,"value-9":1},"V4":{"value-0":-1.25,"value-1":-1,"value-10":1.25,"value-2":-0.75,"value-3":-0.5,"value-4":-0.25,"value-5":0,"value-6":0.25,"value-7":0.5,"value-8":0.75,"value-9":1},"V5":{"value-0":125,"value-1":100,"value-10":125,"value-2":75,"value-3":50,"value-4":25,"value-5":0,"value-6":25,"value-7":50,"value-8":75,"value-9":100},"V6":{"value-0":"2022-01-01T01:01:01.001001Z","value-1":"2022-01-01T02:02:02.002002Z","value-10":"2022-01-01T11:11:11.011011Z","value-2":"2022-01-01T03:03:03.003003Z","value-3":"2022-01-01T04:04:04.004004Z","value-4":"2022-01-01T05:05:05.005005Z","value-5":"2022-01-01T06:06:06.006006Z","value-6":"2022-01-01T07:07:07.007007Z","value-7":"2022-01-01T08:08:08.008008Z","value-8":"2022-01-01T09:09:09.009009Z","value-9":"2022-01-01T10:10:10.01001Z"}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_composite(t *testing.T) {
	cmd := &CatCmd{
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-composite.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"Map":{"-0.25":[{"EmbeddedList":["2022-01-02T11:35:35.035035Z"],"EmbeddedMap":{"0.00":0}}],"-0.50":[{"EmbeddedList":["2022-01-02T08:32:32.032032Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-02T09:33:33.033033Z","2022-01-02T10:34:34.034034Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}}],"-0.75":[{"EmbeddedList":["2022-01-02T02:26:26.026026Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-02T03:27:27.027027Z","2022-01-02T04:28:28.028028Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}},{"EmbeddedList":["2022-01-02T05:29:29.029029Z","2022-01-02T06:30:30.03003Z","2022-01-02T07:31:31.031031Z"],"EmbeddedMap":{"20.00":0.02,"20.01":0.02,"20.02":0.02}}],"-1.00":[{"EmbeddedList":["2022-01-01T16:16:16.016016Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-01T17:17:17.017017Z","2022-01-01T18:18:18.018018Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}},{"EmbeddedList":["2022-01-01T19:19:19.019019Z","2022-01-01T20:20:20.02002Z","2022-01-01T21:21:21.021021Z"],"EmbeddedMap":{"20.00":0.02,"20.01":0.02,"20.02":0.02}},{"EmbeddedList":["2022-01-01T22:22:22.022022Z","2022-01-01T23:23:23.023023Z","2022-01-02T00:24:24.024024Z","2022-01-02T01:25:25.025025Z"],"EmbeddedMap":{"30.00":0.03,"30.01":0.03,"30.02":0.03,"30.03":0.03}}],"-1.25":[{"EmbeddedList":["2022-01-01T01:01:01.001001Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-01T02:02:02.002002Z","2022-01-01T03:03:03.003003Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}},{"EmbeddedList":["2022-01-01T04:04:04.004004Z","2022-01-01T05:05:05.005005Z","2022-01-01T06:06:06.006006Z"],"EmbeddedMap":{"20.00":0.02,"20.01":0.02,"20.02":0.02}},{"EmbeddedList":["2022-01-01T07:07:07.007007Z","2022-01-01T08:08:08.008008Z","2022-01-01T09:09:09.009009Z","2022-01-01T10:10:10.01001Z"],"EmbeddedMap":{"30.00":0.03,"30.01":0.03,"30.02":0.03,"30.03":0.03}},{"EmbeddedList":["2022-01-01T11:11:11.011011Z","2022-01-01T12:12:12.012012Z","2022-01-01T13:13:13.013013Z","2022-01-01T14:14:14.014014Z","2022-01-01T15:15:15.015015Z"],"EmbeddedMap":{"40.00":0.04,"40.01":0.04,"40.02":0.04,"40.03":0.04,"40.04":0.04}}],"0.00":[],"0.25":[{"EmbeddedList":["2022-01-02T12:36:36.036036Z"],"EmbeddedMap":{"0.00":0}}],"0.50":[{"EmbeddedList":["2022-01-02T13:37:37.037037Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-02T14:38:38.038038Z","2022-01-02T15:39:39.039039Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}}],"0.75":[{"EmbeddedList":["2022-01-02T16:40:40.04004Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-02T17:41:41.041041Z","2022-01-02T18:42:42.042042Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}},{"EmbeddedList":["2022-01-02T19:43:43.043043Z","2022-01-02T20:44:44.044044Z","2022-01-02T21:45:45.045045Z"],"EmbeddedMap":{"20.00":0.02,"20.01":0.02,"20.02":0.02}}],"1.00":[{"EmbeddedList":["2022-01-02T22:46:46.046046Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-02T23:47:47.047047Z","2022-01-03T00:48:48.048048Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}},{"EmbeddedList":["2022-01-03T01:49:49.049049Z","2022-01-03T02:50:50.05005Z","2022-01-03T03:51:51.051051Z"],"EmbeddedMap":{"20.00":0.02,"20.01":0.02,"20.02":0.02}},{"EmbeddedList":["2022-01-03T04:52:52.052052Z","2022-01-03T05:53:53.053053Z","2022-01-03T06:54:54.054054Z","2022-01-03T07:55:55.055055Z"],"EmbeddedMap":{"30.00":0.03,"30.01":0.03,"30.02":0.03,"30.03":0.03}}],"1.25":[{"EmbeddedList":["2022-01-03T08:56:56.056056Z"],"EmbeddedMap":{"0.00":0}},{"EmbeddedList":["2022-01-03T09:57:57.057057Z","2022-01-03T10:58:58.058058Z"],"EmbeddedMap":{"10.00":0.01,"10.01":0.01}},{"EmbeddedList":["2022-01-03T11:59:59.059059Z","2022-01-03T13:01:00.06006Z","2022-01-03T14:02:01.061061Z"],"EmbeddedMap":{"20.00":0.02,"20.01":0.02,"20.02":0.02}},{"EmbeddedList":["2022-01-03T15:03:02.062062Z","2022-01-03T16:04:03.063063Z","2022-01-03T17:05:04.064064Z","2022-01-03T18:06:05.065065Z"],"EmbeddedMap":{"30.00":0.03,"30.01":0.03,"30.02":0.03,"30.03":0.03}},{"EmbeddedList":["2022-01-03T19:07:06.066066Z","2022-01-03T20:08:07.067067Z","2022-01-03T21:09:08.068068Z","2022-01-03T22:10:09.069069Z","2022-01-03T23:11:10.07007Z"],"EmbeddedMap":{"40.00":0.04,"40.01":0.04,"40.02":0.04,"40.03":0.04,"40.04":0.04}}]}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}
