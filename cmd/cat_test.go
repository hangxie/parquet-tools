package cmd

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
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

func Test_CatCmd_Run_good_reinterpret_decimal_zero(t *testing.T) {
	cmd := &CatCmd{
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-fields.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":0,"V2":0,"V3":0,"V4":0,"V5":0,"V6":"2022-01-01T00:00:00Z","Ptr":null,"List":[],"MapK":{},"MapV":{}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_fraction(t *testing.T) {
	cmd := &CatCmd{
		Skip:        1,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-fields.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":0.11,"V2":0.11,"V3":0.11,"V4":0.11,"V5":11,"V6":"2022-01-01T01:01:01.001001Z","Ptr":0.11,"List":["0.11"],"MapK":{"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u000b":"value1"},"MapV":{"value1":"0.11"}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_normal(t *testing.T) {
	cmd := &CatCmd{
		Skip:        2,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-fields.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":2.22,"V2":2.22,"V3":2.22,"V4":2.22,"V5":222,"V6":"2022-01-01T02:02:02.002002Z","Ptr":2.22,"List":["2.22","2.22"],"MapK":{"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\ufffd":"value2"},"MapV":{"value1":"2.22","value2":"2.22"}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_negative_zero(t *testing.T) {
	cmd := &CatCmd{
		Skip:        3,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-fields.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":0,"V2":0,"V3":0,"V4":0,"V5":0,"V6":"2022-01-01T03:03:03.003003Z","Ptr":null,"List":[],"MapK":{},"MapV":{}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_negative_fraction(t *testing.T) {
	cmd := &CatCmd{
		Skip:        4,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-fields.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":-0.11,"V2":-0.11,"V3":-0.11,"V4":-0.11,"V5":11,"V6":"2022-01-01T04:04:04.004004Z","Ptr":-0.11,"List":["-0.11"],"MapK":{"\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd":"value1"},"MapV":{"value1":"-0.11"}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_CatCmd_Run_good_reinterpret_decimal_negative_normal(t *testing.T) {
	cmd := &CatCmd{
		Skip:        5,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/reinterpret-fields.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t,
		`{"V1":-2.22,"V2":-2.22,"V3":-2.22,"V4":-2.22,"V5":222,"V6":"2022-01-01T05:05:05.005005Z","Ptr":-2.22,"List":["-2.22","-2.22"],"MapK":{"\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\"":"value2"},"MapV":{"value1":"-2.22","value2":"-2.22"}}`+"\n",
		stdout)
	assert.Equal(t, "", stderr)
}

func Test_cat_reformatStringDecimalValue_good_decimal(t *testing.T) {
	fieldAttr := ReinterpretField{
		parquetType:   parquet.Type_BYTE_ARRAY,
		convertedType: parquet.ConvertedType_DECIMAL,
		scale:         2,
		precision:     10,
	}

	decimalValue := types.StrIntToBinary("-011", "BigEndian", 0, true)
	reformatStringValue(fieldAttr, reflect.ValueOf(&decimalValue).Elem())
	assert.Equal(t, "-0.11", decimalValue)

	decimalPtr := new(string)
	*decimalPtr = types.StrIntToBinary("222", "BigEndian", 0, true)
	reformatStringValue(fieldAttr, reflect.ValueOf(&decimalPtr).Elem())
	assert.Equal(t, "2.22", *decimalPtr)

	var nilPtr *string
	reformatStringValue(fieldAttr, reflect.ValueOf(&nilPtr).Elem())
	assert.Nil(t, nilPtr)
}

func Test_cat_reformatStringDecimalValue_good_interval(t *testing.T) {
	fieldAttr := ReinterpretField{
		parquetType:   parquet.Type_BYTE_ARRAY,
		convertedType: parquet.ConvertedType_INTERVAL,
		scale:         0,
		precision:     10,
	}

	intervalValue := types.StrIntToBinary("54321", "LittleEndian", 10, false)
	assert.NotEqual(t, "54321", intervalValue)

	reformatStringValue(fieldAttr, reflect.ValueOf(&intervalValue).Elem())
	assert.Equal(t, "54321", intervalValue)
}

func Test_cat_reformatStringDecimalValue_good_int96(t *testing.T) {
	fieldAttr := ReinterpretField{
		parquetType:   parquet.Type_INT96,
		convertedType: parquet.ConvertedType_TIMESTAMP_MICROS,
		scale:         0,
		precision:     0,
	}

	timeValue, _ := time.Parse("2006-01-02", "2022-01-01")
	int96Value := types.TimeToINT96(timeValue)
	assert.NotEqual(t, "2022-01-01T00:00:00Z", int96Value)

	reformatStringValue(fieldAttr, reflect.ValueOf(&int96Value).Elem())
	assert.Equal(t, "2022-01-01T00:00:00Z", int96Value)
}
