package main

import (
	"context"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserInjectsCanceledContext(t *testing.T) {
	parser := newParser(new(cli))
	kongCtx, err := parser.Parse([]string{"row-count", "testdata/good.parquet"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, run(kongCtx, ctx), context.Canceled)
}

func TestWriterFinalizationPreservesCancellation(t *testing.T) {
	err := fs.WalkDir(os.DirFS("cmd"), ".", func(path string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		source, err := fs.ReadFile(os.DirFS("cmd"), path)
		require.NoError(t, err)
		if strings.Contains(string(source), "WriteStopWithContext(context.WithoutCancel(") {
			t.Errorf("%s detaches writer finalization from cancellation", path)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestParserValueMode(t *testing.T) {
	for _, command := range []string{"cat", "import"} {
		for _, mode := range []string{"", "interpreted", "raw", "auto"} {
			t.Run(command+"/"+mode, func(t *testing.T) {
				options := []string{command}
				if command == "import" {
					options = append(options, "-s", "input.json", "-m", "schema.json")
				}
				if mode != "" {
					options = append(options, "--value-mode", mode)
				}
				options = append(options, "file.parquet")
				args := new(cli)
				_, err := newParser(args).Parse(options)
				if mode == "auto" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				expected := mode
				if expected == "" {
					expected = "interpreted"
				}
				if command == "cat" {
					require.Equal(t, expected, args.Cat.ValueMode)
				} else {
					require.Equal(t, expected, args.Import.ValueMode)
				}
			})
		}
	}
}
