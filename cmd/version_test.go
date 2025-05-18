package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_versionCmd(t *testing.T) {
	testCases := map[string]struct {
		cmd    VersionCmd
		stdout string
	}{
		"plain":             {cmd: VersionCmd{false, false, false, false}, stdout: "v1.2.3\n"},
		"plain-with-build":  {cmd: VersionCmd{false, false, true, false}, stdout: "v1.2.3\ntoday\n"},
		"plain-with-source": {cmd: VersionCmd{false, false, false, true}, stdout: "v1.2.3\nUT\n"},
		"plain-with-all":    {cmd: VersionCmd{false, true, false, false}, stdout: "v1.2.3\ntoday\nUT\n"},
		"json":              {cmd: VersionCmd{true, false, false, false}, stdout: `{"Version":"v1.2.3"}` + "\n"},
		"json-with-build":   {cmd: VersionCmd{true, false, true, false}, stdout: `{"Version":"v1.2.3","BuildTime":"today"}` + "\n"},
		"json-with-source":  {cmd: VersionCmd{true, false, false, true}, stdout: `{"Version":"v1.2.3","Source":"UT"}` + "\n"},
		"json-with-all":     {cmd: VersionCmd{true, true, false, false}, stdout: `{"Version":"v1.2.3","BuildTime":"today","Source":"UT"}` + "\n"},
	}

	version = "v1.2.3"
	build = "today"
	source = "UT"

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			stdout, stderr := captureStdoutStderr(func() {
				require.Nil(t, tc.cmd.Run())
			})
			require.Equal(t, tc.stdout, stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func Benchmark_VersionCmd_Run(b *testing.B) {
	savedStdout, savedStderr := os.Stdout, os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout, os.Stderr = savedStdout, savedStderr
		_ = devNull.Close()
	}()

	cmd := VersionCmd{
		All: true,
	}
	b.Run("default", func(b *testing.B) {
		require.NoError(b, cmd.Run())
	})
}
