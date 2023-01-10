package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_SizeCmd_Run_non_existent_file(t *testing.T) {
	cmd := &SizeCmd{
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "file/does/not/exist",
			},
		},
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to open local")
}

func Test_SizeCmd_Run_invalid_query(t *testing.T) {
	cmd := &SizeCmd{
		Query: "invalid",
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	err := cmd.Run(&Context{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unknown query type")
}

func Test_SizeCmd_Run_good_raw(t *testing.T) {
	cmd := &SizeCmd{
		Query: "raw",
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "17817\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_raw_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "raw",
		JSON:  true,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Raw":17817}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_uncompressed(t *testing.T) {
	cmd := &SizeCmd{
		Query: "uncompressed",
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "26527\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_uncompressed_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "uncompressed",
		JSON:  true,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Uncompressed":26527}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_footer(t *testing.T) {
	cmd := &SizeCmd{
		Query: "footer",
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "6400\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_footer_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "footer",
		JSON:  true,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Footer":6400}`+"\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_all(t *testing.T) {
	cmd := &SizeCmd{
		Query: "all",
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, "17817 26527 6400\n", stdout)
	require.Equal(t, "", stderr)
}

func Test_SizeCmd_Run_good_all_json(t *testing.T) {
	cmd := &SizeCmd{
		Query: "all",
		JSON:  true,
		ReadOption: ReadOption{
			CommonOption: CommonOption{
				URI: "testdata/all-types.parquet",
			},
		},
	}

	stdout, stderr := captureStdoutStderr(func() {
		require.Nil(t, cmd.Run(&Context{}))
	})
	require.Equal(t, `{"Raw":17817,"Uncompressed":26527,"Footer":6400}`+"\n", stdout)
	require.Equal(t, "", stderr)
}
