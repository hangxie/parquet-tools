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
			URI: "testdata/all-types.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Bool":true,"Int32":0,"Int64":0,"Int96":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Float":0,"Double":0,"Bytearray":"ByteArray","FixedLenByteArray":"HelloWorld","Utf8":"utf8","Int_8":0,"Int_16":0,"Int_32":0,"Int_64":0,"Uint_8":0,"Uint_16":0,"Uint_32":0,"Uint_64":0,"Date":0,"Date2":0,"Timemillis":0,"Timemillis2":0,"Timemicros":0,"Timemicros2":0,"Timestampmillis":0,"Timestampmillis2":0,"Timestampmicros":0,"Timestampmicros2":0,"Interval":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Decimal1":123.45,"Decimal2":123.45,"Decimal3":0,"Decimal4":0,"Decimal5":0,"Decimal_pointer":null,"Map":{"One":1,"Two":2},"List":["item1","item2"],"Repeated":[1,2,3],"NestedMap":{},"NestedList":[]}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_fraction(t *testing.T) {
	cmd := &CatCmd{
		Skip:        1,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Bool":false,"Int32":1,"Int64":1,"Int96":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Float":0.5,"Double":0.5,"Bytearray":"ByteArray","FixedLenByteArray":"HelloWorld","Utf8":"utf8","Int_8":1,"Int_16":1,"Int_32":1,"Int_64":1,"Uint_8":1,"Uint_16":1,"Uint_32":1,"Uint_64":1,"Date":1,"Date2":0,"Timemillis":1,"Timemillis2":0,"Timemicros":1,"Timemicros2":0,"Timestampmillis":1,"Timestampmillis2":0,"Timestampmicros":1,"Timestampmicros2":0,"Interval":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Decimal1":123.45,"Decimal2":123.45,"Decimal3":0.11,"Decimal4":0.11,"Decimal5":0,"Decimal_pointer":0.11,"Map":{"One":1,"Two":2},"List":["item1","item2"],"Repeated":[1,2,3],"NestedMap":{},"NestedList":[]}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_normal(t *testing.T) {
	cmd := &CatCmd{
		Skip:        2,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Bool":true,"Int32":2,"Int64":2,"Int96":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Float":1,"Double":1,"Bytearray":"ByteArray","FixedLenByteArray":"HelloWorld","Utf8":"utf8","Int_8":2,"Int_16":2,"Int_32":2,"Int_64":2,"Uint_8":2,"Uint_16":2,"Uint_32":2,"Uint_64":2,"Date":2,"Date2":0,"Timemillis":2,"Timemillis2":0,"Timemicros":2,"Timemicros2":0,"Timestampmillis":2,"Timestampmillis2":0,"Timestampmicros":2,"Timestampmicros2":0,"Interval":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Decimal1":123.45,"Decimal2":123.45,"Decimal3":2.22,"Decimal4":2.22,"Decimal5":0,"Decimal_pointer":2.22,"Map":{"One":1,"Two":2},"List":["item1","item2"],"Repeated":[1,2,3],"NestedMap":{},"NestedList":[]}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_negative_zero(t *testing.T) {
	cmd := &CatCmd{
		Skip:        3,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Bool":false,"Int32":3,"Int64":3,"Int96":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Float":1.5,"Double":1.5,"Bytearray":"ByteArray","FixedLenByteArray":"HelloWorld","Utf8":"utf8","Int_8":3,"Int_16":3,"Int_32":3,"Int_64":3,"Uint_8":3,"Uint_16":3,"Uint_32":3,"Uint_64":3,"Date":3,"Date2":0,"Timemillis":3,"Timemillis2":0,"Timemicros":3,"Timemicros2":0,"Timestampmillis":3,"Timestampmillis2":0,"Timestampmicros":3,"Timestampmicros2":0,"Interval":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Decimal1":123.45,"Decimal2":123.45,"Decimal3":0,"Decimal4":0,"Decimal5":0,"Decimal_pointer":null,"Map":{"One":1,"Two":2},"List":["item1","item2"],"Repeated":[1,2,3],"NestedMap":{},"NestedList":[]}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_negative_fraction(t *testing.T) {
	cmd := &CatCmd{
		Skip:        4,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Bool":true,"Int32":4,"Int64":4,"Int96":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Float":2,"Double":2,"Bytearray":"ByteArray","FixedLenByteArray":"HelloWorld","Utf8":"utf8","Int_8":4,"Int_16":4,"Int_32":4,"Int_64":4,"Uint_8":4,"Uint_16":4,"Uint_32":4,"Uint_64":4,"Date":4,"Date2":0,"Timemillis":4,"Timemillis2":0,"Timemicros":4,"Timemicros2":0,"Timestampmillis":4,"Timestampmillis2":0,"Timestampmicros":4,"Timestampmicros2":0,"Interval":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Decimal1":123.45,"Decimal2":123.45,"Decimal3":-0.11,"Decimal4":-0.11,"Decimal5":0,"Decimal_pointer":-0.11,"Map":{"One":1,"Two":2},"List":["item1","item2"],"Repeated":[1,2,3],"NestedMap":{},"NestedList":[]}`+"\n")
	assert.Equal(t, stderr, "")
}

func Test_CatCmd_Run_good_decimal_negative_normal(t *testing.T) {
	cmd := &CatCmd{
		Skip:        5,
		Limit:       1,
		PageSize:    10,
		SampleRatio: 1.0,
		CommonOption: CommonOption{
			URI: "testdata/all-types.parquet",
		},
		Format: "jsonl",
	}

	stdout, stderr := captureStdoutStderr(func() {
		assert.Nil(t, cmd.Run(&Context{}))
	})
	assert.Equal(t, stdout, `{"Bool":false,"Int32":5,"Int64":5,"Int96":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Float":2.5,"Double":2.5,"Bytearray":"ByteArray","FixedLenByteArray":"HelloWorld","Utf8":"utf8","Int_8":5,"Int_16":5,"Int_32":5,"Int_64":5,"Uint_8":5,"Uint_16":5,"Uint_32":5,"Uint_64":5,"Date":5,"Date2":0,"Timemillis":5,"Timemillis2":0,"Timemicros":5,"Timemicros2":0,"Timestampmillis":5,"Timestampmillis2":0,"Timestampmicros":5,"Timestampmicros2":0,"Interval":"90\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","Decimal1":123.45,"Decimal2":123.45,"Decimal3":-2.22,"Decimal4":-2.22,"Decimal5":0,"Decimal_pointer":-2.22,"Map":{"One":1,"Two":2},"List":["item1","item2"],"Repeated":[1,2,3],"NestedMap":{},"NestedList":[]}`+"\n")
	assert.Equal(t, stderr, "")
}
