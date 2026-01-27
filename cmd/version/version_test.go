package version

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
)

func TestCmd(t *testing.T) {
	testCases := map[string]struct {
		cmd    Cmd
		stdout string
	}{
		"plain":             {cmd: Cmd{JSON: false, All: false, BuildTime: false, Source: false}, stdout: "v1.2.3\n"},
		"plain-with-build":  {cmd: Cmd{JSON: false, All: false, BuildTime: true, Source: false}, stdout: "v1.2.3\ntoday\n"},
		"plain-with-source": {cmd: Cmd{JSON: false, All: false, BuildTime: false, Source: true}, stdout: "v1.2.3\nUT\n"},
		"plain-with-all":    {cmd: Cmd{JSON: false, All: true, BuildTime: false, Source: false}, stdout: "v1.2.3\ntoday\nUT\n"},
		"json":              {cmd: Cmd{JSON: true, All: false, BuildTime: false, Source: false}, stdout: `{"Version":"v1.2.3"}` + "\n"},
		"json-with-build":   {cmd: Cmd{JSON: true, All: false, BuildTime: true, Source: false}, stdout: `{"Version":"v1.2.3","BuildTime":"today"}` + "\n"},
		"json-with-source":  {cmd: Cmd{JSON: true, All: false, BuildTime: false, Source: true}, stdout: `{"Version":"v1.2.3","Source":"UT"}` + "\n"},
		"json-with-all":     {cmd: Cmd{JSON: true, All: true, BuildTime: false, Source: false}, stdout: `{"Version":"v1.2.3","BuildTime":"today","Source":"UT"}` + "\n"},
	}

	version = "v1.2.3"
	build = "today"
	source = "UT"

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			stdout, stderr := testutils.CaptureStdoutStderr(func() {
				require.Nil(t, tc.cmd.Run())
			})
			require.Equal(t, tc.stdout, stdout)
			require.Equal(t, "", stderr)
		})
	}
}

func BenchmarkVersionCmd(b *testing.B) {
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

	cmd := Cmd{
		All: true,
	}
	b.Run("default", func(b *testing.B) {
		for b.Loop() {
			require.NoError(b, cmd.Run())
		}
	})
}
