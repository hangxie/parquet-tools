package main

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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

func TestRunNamesLimitFlag(t *testing.T) {
	parser := newParser(new(cli))
	kongCtx, err := parser.Parse([]string{"cat", "--format", "jsonl", "testdata/delta-byte-array-bomb.parquet"})
	require.NoError(t, err)

	err = run(kongCtx, context.Background())
	require.ErrorContains(t, err, "(see --max-decoded-batch-bytes)")
	require.NotContains(t, err.Error(), "reader.WithMaxDecodedBatchBytes")
}

func TestParserLimitFlags(t *testing.T) {
	parser := newParser(new(cli))
	_, err := parser.Parse([]string{"cat", "--max-page-size=-1", "--max-decompressed-size", "536870912", "--max-decoded-batch-bytes=-1", "testdata/good.parquet"})
	require.NoError(t, err)
}

func TestWriterFinalizationPreservesCancellation(t *testing.T) {
	for _, root := range []string{"cmd", "io"} {
		err := fs.WalkDir(os.DirFS(root), ".", func(path string, entry fs.DirEntry, err error) error {
			require.NoError(t, err)
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			source, err := fs.ReadFile(os.DirFS(root), path)
			require.NoError(t, err)
			if strings.Contains(string(source), "WriteStopWithContext(context.WithoutCancel(") {
				t.Errorf("%s/%s detaches writer finalization from cancellation", root, path)
			}
			return nil
		})
		require.NoError(t, err)
	}
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

func TestParserKeySourcesMutuallyExclusive(t *testing.T) {
	readPlain := []string{"--footer-key=a", "--aad-prefix=a", "--column-key=c=a"}
	readEnv := []string{"--footer-key-env=A", "--aad-prefix-env=A", "--column-key-env=c=A"}
	readFile := []string{"--key-file=keys.json"}
	writePlain := []string{"--writer-footer-key=a", "--writer-aad-prefix=a", "--writer-column-key=c=a"}
	writeEnv := []string{"--writer-footer-key-env=A", "--writer-aad-prefix-env=A", "--writer-column-key-env=c=A"}
	writeFile := []string{"--writer-key-file=keys.json"}

	testCases := map[string]struct {
		base []string
		sets [][]string
	}{
		"reader": {[]string{"cat", "testdata/good.parquet"}, [][]string{readPlain, readEnv, readFile}},
		"writer": {[]string{"transcode", "-s", "testdata/good.parquet", "out.parquet"}, [][]string{writePlain, writeEnv, writeFile}},
	}

	parse := func(base []string, flags ...string) error {
		_, err := newParser(new(cli)).Parse(append(append([]string{}, base...), flags...))
		return err
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for i, set := range tc.sets {
				// flags of one set may all be used together
				require.NoError(t, parse(tc.base, set...), set)

				// any flag of one set conflicts with any flag of another
				for _, other := range tc.sets[i+1:] {
					for _, a := range set {
						for _, b := range other {
							require.ErrorContains(t, parse(tc.base, a, b), "can't be used together", "%s %s", a, b)
						}
					}
				}
			}
		})
	}

	t.Run("reader-and-writer-independent", func(t *testing.T) {
		require.NoError(t, parse([]string{"transcode", "-s", "testdata/good.parquet", "out.parquet"}, "--key-file=keys.json", "--writer-footer-key-env=A"))
	})

	// --writer-supply-aad-prefix is a switch, not a key, so it goes with any key source
	t.Run("supply-aad-prefix-with-any-source", func(t *testing.T) {
		base := []string{"transcode", "-s", "testdata/good.parquet", "out.parquet", "--writer-supply-aad-prefix"}
		for _, set := range [][]string{writePlain, writeEnv, writeFile} {
			require.NoError(t, parse(base, set...), set)
		}
	})
}

// runCLI parses and runs one command line, returning what it wrote to stdout.
func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	kongCtx, err := newParser(new(cli)).Parse(args)
	require.NoError(t, err)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	output := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(r)
		_ = r.Close()
		output <- string(data)
	}()

	runErr := func() error {
		// deferred so stdout comes back even if the command panics
		saved := os.Stdout
		os.Stdout = w
		defer func() {
			os.Stdout = saved
			_ = w.Close()
		}()
		return run(kongCtx, context.Background())
	}()
	return <-output, runErr
}

// Keys named by variables must survive the whole path: encrypt on write, decrypt on read.
func TestEncryptionRoundTripWithKeysFromEnv(t *testing.T) {
	t.Setenv("PT_E2E_FOOTER", "MDEyMzQ1Njc4OTAxMjM0NQ==")
	t.Setenv("PT_E2E_COLUMN", "MTIzNDU2Nzg5MDEyMzQ1MA==")
	const source = "testdata/good.parquet"

	original, err := runCLI(t, "cat", "--format", "jsonl", source)
	require.NoError(t, err)
	require.NotEmpty(t, original)

	keyFile := filepath.Join(t.TempDir(), "keys.json")
	require.NoError(t, os.WriteFile(keyFile, []byte(`{"footer_key_env":"PT_E2E_FOOTER","column_keys_env":{"shoe_name":"PT_E2E_COLUMN"}}`), 0o600))

	flags := func(prefix string) []string {
		return []string{"--" + prefix + "footer-key-env", "PT_E2E_FOOTER", "--" + prefix + "column-key-env", "shoe_name=PT_E2E_COLUMN"}
	}
	testCases := map[string]struct {
		writeKeys []string
		readKeys  []string
	}{
		"flags":                {flags("writer-"), flags("")},
		"key-file":             {[]string{"--writer-key-file", keyFile}, []string{"--key-file", keyFile}},
		"flags-then-key-file":  {flags("writer-"), []string{"--key-file", keyFile}},
		"key-file-then-flags":  {[]string{"--writer-key-file", keyFile}, flags("")},
		"plain-then-env-flags": {[]string{"--writer-footer-key", "MDEyMzQ1Njc4OTAxMjM0NQ==", "--writer-column-key", "shoe_name=MTIzNDU2Nzg5MDEyMzQ1MA=="}, flags("")},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			encrypted := filepath.Join(t.TempDir(), "encrypted.parquet")
			_, err := runCLI(t, append([]string{"transcode", "-s", source, encrypted}, tc.writeKeys...)...)
			require.NoError(t, err)

			_, err = runCLI(t, "cat", "--format", "jsonl", encrypted)
			require.ErrorContains(t, err, "decryption key required")

			decrypted, err := runCLI(t, append([]string{"cat", "--format", "jsonl", encrypted}, tc.readKeys...)...)
			require.NoError(t, err)
			require.Equal(t, original, decrypted)
		})
	}
}

// The writer AAD prefix binds the output file; a supplied --aad-prefix must match it, and with --writer-supply-aad-prefix readers must pass it back.
func TestWriterAADPrefixRoundTrip(t *testing.T) {
	const footerKey = "MDEyMzQ1Njc4OTAxMjM0NQ=="
	const prefix = "dGVzdGVy"
	t.Setenv("PT_AAD_FOOTER", footerKey)
	t.Setenv("PT_AAD_PREFIX", prefix)
	const source = "testdata/good.parquet"

	original, err := runCLI(t, "cat", "--format", "jsonl", source)
	require.NoError(t, err)

	keyFile := filepath.Join(t.TempDir(), "keys.json")
	require.NoError(t, os.WriteFile(keyFile, []byte(`{"footer_key_env":"PT_AAD_FOOTER","aad_prefix_env":"PT_AAD_PREFIX"}`), 0o600))

	testCases := map[string]struct {
		writeKeys []string
		supplied  bool
	}{
		"stored-plain-flags":   {[]string{"--writer-footer-key", footerKey, "--writer-aad-prefix", prefix}, false},
		"supplied-plain-flags": {[]string{"--writer-footer-key", footerKey, "--writer-aad-prefix", prefix, "--writer-supply-aad-prefix"}, true},
		"supplied-env-flags":   {[]string{"--writer-footer-key-env", "PT_AAD_FOOTER", "--writer-aad-prefix-env", "PT_AAD_PREFIX", "--writer-supply-aad-prefix"}, true},
		"supplied-key-file":    {[]string{"--writer-key-file", keyFile, "--writer-supply-aad-prefix"}, true},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			encrypted := filepath.Join(t.TempDir(), "encrypted.parquet")
			_, err := runCLI(t, append([]string{"transcode", "-s", source, encrypted}, tc.writeKeys...)...)
			require.NoError(t, err)

			read := func(extra ...string) (string, error) {
				return runCLI(t, append([]string{"cat", "--format", "jsonl", "--footer-key", footerKey, encrypted}, extra...)...)
			}
			got, err := read()
			if tc.supplied {
				require.ErrorContains(t, err, "AAD prefix is required")
				_, err = read("--aad-prefix", "d3Jvbmc=")
				require.ErrorContains(t, err, "message authentication failed")
			} else {
				require.NoError(t, err)
				require.Equal(t, original, got)
				_, err = read("--aad-prefix", "d3Jvbmc=")
				require.ErrorContains(t, err, "AAD prefix mismatch")
			}

			got, err = read("--aad-prefix", prefix)
			require.NoError(t, err)
			require.Equal(t, original, got)
		})
	}

	t.Run("no-prefix-in-file", func(t *testing.T) {
		encrypted := filepath.Join(t.TempDir(), "encrypted.parquet")
		_, err := runCLI(t, "transcode", "-s", source, encrypted, "--writer-footer-key", footerKey)
		require.NoError(t, err)

		_, err = runCLI(t, "cat", "--format", "jsonl", "--footer-key", footerKey, "--aad-prefix", prefix, encrypted)
		require.ErrorContains(t, err, "AAD prefix supplied but not used")
	})
}
