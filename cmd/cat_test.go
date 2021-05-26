package cmd

import (
	"encoding/json"
	"errors"
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

func Test_CatCmd_matchRowFunc_invalid_filter(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "invalid filter"
	f, err := cmd.matchRowFunc()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse filter")
	assert.Nil(t, f)
}

func Test_CatCmd_matchRowFunc_invalid_operator(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a><b"
	f, err := cmd.matchRowFunc()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid operator")
	assert.Nil(t, f)
}

func Test_CatCmd_matchRowFunc_bad_value(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value string
		err   error
	}{
		{"1", nil},
		{"0.2", nil},
		{`"1"`, nil},
		{`""`, nil},
		{"nil", nil},
		{"niL", nil},
		{"null", nil},
		{"Null", nil},
		{`"`, errors.New("single quote")},
		{"", errors.New("missing value in filter")},
		{"not-a-number", errors.New("")},
		{`"missing-quote`, errors.New("missing trailing quote")},
		{`missing-quote"`, errors.New("missing leading quote")},
		{`not-a-number`, errors.New("not a numeric value")},
	}

	for _, tc := range testCases {
		cmd.Filter = "a<>" + tc.value
		t.Logf("testing [%s]", cmd.Filter)
		f, err := cmd.matchRowFunc()
		if tc.err == nil {
			assert.Nil(t, err)
			assert.NotNil(t, f)
		} else {
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), tc.err.Error())
			assert.Nil(t, f)
		}
	}
}

func Test_CatCmd_matchRowFunc_bad_json(t *testing.T) {
	cmd := &CatCmd{}

	cmd.Filter = "a<>0"
	f, err := cmd.matchRowFunc()
	assert.Nil(t, err)
	assert.NotNil(t, f)

	_, err = f([]byte("bad json"))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to parse JSON string")
}

func Test_CatCmd_matchRowFunc_missing_layer(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []string{
		`{"z":{}}`,
		`{"a":1}`,
		`{"a":{"z":{"c":1}}}`,
		`{"a":{"b":{"z":1}}}`,
	}

	for op := range supportedOperators {
		cmd.Filter = "a.b.c" + op + `"a"`
		f, err := cmd.matchRowFunc()
		assert.Nil(t, err)
		assert.NotNil(t, f)
		for _, tc := range testCases {
			t.Logf("testing [%s] '%s''", cmd.Filter, tc)
			matched, err := f([]byte(tc))
			assert.Nil(t, err)
			// missing layer means not equal, all other comparisons are just false
			assert.Equal(t, matched, op == "<>")
		}
	}
}

func Test_CatCmd_matchRowFunc_nil(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value string
		isNil bool
	}{
		{`{"a":{"b":12}}`, false},
		{`{"a":{"b":null}}`, true},
	}

	for _, nilValue := range []string{" nil", "NIL ", "null", "  NULL"} {
		for op := range supportedOperators {
			cmd.Filter = "a.b " + op + nilValue
			f, err := cmd.matchRowFunc()
			assert.Nil(t, err)
			assert.NotNil(t, f)
			for _, tc := range testCases {
				t.Logf("testing [%s] '%s''", cmd.Filter, tc.value)
				matched, err := f([]byte(tc.value))
				assert.Nil(t, err)
				// nill only support equal and not equal, all other comparisons are just false
				switch op {
				case "==":
					assert.Equal(t, matched, tc.isNil)
				case "<>":
					assert.Equal(t, matched, !tc.isNil)
				default:
					assert.Equal(t, matched, false)
				}
			}
		}
	}
}

func Test_CatCmd_matchRowFunc_number_equal_not_equal(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value    string
		isEleven bool
	}{
		{`{"a":{"b":12}}`, false},
		{`{"a":{"b":"12"}}`, false},
		{`{"a":{"b":{"c":"12"}}}`, false},
		{`{"a":{"b":[1,2,3]}}`, false},
		{`{"a":{"b":[11]}}`, false},
		{`{"a":{"b":{"c":"11"}}}`, false},
		{`{"a":{"b":"11"}}`, false},
		{`{"a":{"b":11}}`, true},
	}

	for _, op := range []string{"==", "<>"} {
		cmd.Filter = "a.b" + op + "11"
		f, err := cmd.matchRowFunc()
		assert.Nil(t, err)
		assert.NotNil(t, f)
		for _, tc := range testCases {
			t.Logf("testing [%s] '%s''", cmd.Filter, tc.value)
			matched, err := f([]byte(tc.value))
			assert.Nil(t, err)
			if op == "==" {
				assert.Equal(t, matched, tc.isEleven)
			} else {
				assert.Equal(t, matched, !tc.isEleven)
			}
		}
	}
}

func Test_CatCmd_matchRowFunc_string_equal_not_equal(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value    string
		isEleven bool
	}{
		{`{"a":{"b":12}}`, false},
		{`{"a":{"b":"12"}}`, false},
		{`{"a":{"b":{"c":"12"}}}`, false},
		{`{"a":{"b":[1,2,3]}}`, false},
		{`{"a":{"b":[11]}}`, false},
		{`{"a":{"b":{"c":"11"}}}`, false},
		{`{"a":{"b":11}}`, false},
		{`{"a":{"b":"11"}}`, true},
	}

	for _, op := range []string{"==", "<>"} {
		cmd.Filter = "a.b" + op + `"11"`
		f, err := cmd.matchRowFunc()
		assert.Nil(t, err)
		assert.NotNil(t, f)
		for _, tc := range testCases {
			t.Logf("testing [%s] '%s''", cmd.Filter, tc.value)
			matched, err := f([]byte(tc.value))
			assert.Nil(t, err)
			if op == "==" {
				assert.Equal(t, matched, tc.isEleven)
			} else {
				assert.Equal(t, matched, !tc.isEleven)
			}
		}
	}
}

func Test_CatCmd_matchRowFunc_number_gt_le(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value         string
		isGreaterThan bool
	}{
		{`{"a":{"b":10.99}}`, false},
		{`{"a":{"b":11}}`, false},
		{`{"a":{"b":12}}`, true},
	}

	for _, op := range []string{">", "<="} {
		cmd.Filter = "a.b" + op + "11"
		f, err := cmd.matchRowFunc()
		assert.Nil(t, err)
		assert.NotNil(t, f)
		for _, tc := range testCases {
			t.Logf("testing [%s] '%s''", cmd.Filter, tc.value)
			matched, err := f([]byte(tc.value))
			assert.Nil(t, err)
			if op == ">" {
				assert.Equal(t, tc.isGreaterThan, matched)
			} else {
				assert.Equal(t, tc.isGreaterThan, !matched)
			}
		}
	}
}

func Test_CatCmd_matchRowFunc_string_gt_le(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value         string
		isGreaterThan bool
	}{
		{`{"a":{"b":"aa"}}`, false},
		{`{"a":{"b":"ab"}}`, false},
		{`{"a":{"b":"ab "}}`, true},
		{`{"a":{"b":"bb"}}`, true},
	}

	for _, op := range []string{">", "<="} {
		cmd.Filter = "a.b" + op + `"ab"`
		f, err := cmd.matchRowFunc()
		assert.Nil(t, err)
		assert.NotNil(t, f)
		for _, tc := range testCases {
			t.Logf("testing [%s] '%s''", cmd.Filter, tc.value)
			matched, err := f([]byte(tc.value))
			assert.Nil(t, err)
			if op == ">" {
				assert.Equal(t, tc.isGreaterThan, matched)
			} else {
				assert.Equal(t, tc.isGreaterThan, !matched)
			}
		}
	}
}

func Test_CatCmd_matchRowFunc_number_lt_ge(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value      string
		isLessThan bool
	}{
		{`{"a":{"b":10.99}}`, true},
		{`{"a":{"b":11}}`, false},
		{`{"a":{"b":12}}`, false},
	}

	for _, op := range []string{"<", ">="} {
		cmd.Filter = "a.b" + op + "11"
		f, err := cmd.matchRowFunc()
		assert.Nil(t, err)
		assert.NotNil(t, f)
		for _, tc := range testCases {
			t.Logf("testing [%s] '%s''", cmd.Filter, tc.value)
			matched, err := f([]byte(tc.value))
			assert.Nil(t, err)
			if op == "<" {
				assert.Equal(t, tc.isLessThan, matched)
			} else {
				assert.Equal(t, tc.isLessThan, !matched)
			}
		}
	}
}

func Test_CatCmd_matchRowFunc_string_lt_ge(t *testing.T) {
	cmd := &CatCmd{}
	testCases := []struct {
		value      string
		isLessThan bool
	}{
		{`{"a":{"b":"aa"}}`, true},
		{`{"a":{"b":"ab"}}`, false},
		{`{"a":{"b":"ab "}}`, false},
		{`{"a":{"b":"bb"}}`, false},
	}

	for _, op := range []string{"<", ">="} {
		cmd.Filter = "a.b" + op + `"ab"`
		f, err := cmd.matchRowFunc()
		assert.Nil(t, err)
		assert.NotNil(t, f)
		for _, tc := range testCases {
			t.Logf("testing [%s] '%s''", cmd.Filter, tc.value)
			matched, err := f([]byte(tc.value))
			assert.Nil(t, err)
			if op == "<" {
				assert.Equal(t, tc.isLessThan, matched)
			} else {
				assert.Equal(t, tc.isLessThan, !matched)
			}
		}
	}
}

func Test_CatCmd_Run_good_filter_equal(t *testing.T) {
	cmd := &CatCmd{
		Limit:       10,
		PageSize:    10,
		SampleRatio: 1.0,
		Filter:      `Shoe_brand == "nike"`,
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
		Filter:      `  Shoe_brand <>  "nike"  `,
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
