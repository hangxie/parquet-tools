package testutils

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/hangxie/parquet-tools/cmd/schema"
	pio "github.com/hangxie/parquet-tools/io"
)

var stdCaptureMutex sync.Mutex

// HasSameSchema compares the schema of two parquet files
func HasSameSchema(file1, file2 string, ignoreEncoding, ignoreCompression bool) bool {
	getSchema := func(file string) string {
		cmd := schema.Cmd{
			ReadOption: pio.ReadOption{},
			URI:        file,
			Format:     "json",
		}
		stdout, _ := CaptureStdoutStderr(func() {
			_ = cmd.Run()
		})
		return stdout
	}

	schema1 := getSchema(file1)
	schema2 := getSchema(file2)

	if ignoreEncoding {
		re := regexp.MustCompile(`, encoding=[A-Z0-9_]+`)
		schema1 = re.ReplaceAllString(schema1, "")
		schema2 = re.ReplaceAllString(schema2, "")
	}

	if ignoreCompression {
		re := regexp.MustCompile(`, compression=[A-Z0-9_]+`)
		schema1 = re.ReplaceAllString(schema1, "")
		schema2 = re.ReplaceAllString(schema2, "")
	}

	return schema1 == schema2
}

// CaptureStdoutStderr - thread-safe version using mutex
func CaptureStdoutStderr(f func()) (string, string) {
	stdCaptureMutex.Lock()
	defer stdCaptureMutex.Unlock()

	savedStdout := os.Stdout
	savedStderr := os.Stderr

	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = wOut
	os.Stderr = wErr
	f()
	_ = wOut.Close()
	_ = wErr.Close()
	stdout, _ := io.ReadAll(rOut)
	stderr, _ := io.ReadAll(rErr)
	_ = rOut.Close()
	_ = rErr.Close()

	os.Stdout = savedStdout
	os.Stderr = savedStderr

	return string(stdout), string(stderr)
}

// LoadExpected
func LoadExpected(t *testing.T, fileName string) string {
	buf, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal("cannot load golden file:", fileName, "because of:", err.Error())
	}
	if !strings.HasSuffix(fileName, ".json") && !strings.HasSuffix(fileName, ".jsonl") {
		return string(buf)
	}

	// JSON and JSONL golden files are formatted by jq
	var result string
	var currentBuf []byte
	for _, line := range bytes.Split(buf, []byte("\n")) {
		// in jq format, if the first character is not space than it's
		// start (when currentBuf is empty) or end of an object (when
		// currentBuf is not empty)
		endOfObject := len(line) > 0 && line[0] != ' ' && len(currentBuf) != 0
		currentBuf = append(currentBuf, line...)
		if endOfObject {
			dst := new(bytes.Buffer)
			if err := json.Compact(dst, currentBuf); err != nil {
				t.Fatal("cannot parse golden file:", fileName, "because of:", err.Error())
			}
			result += dst.String() + "\n"
			currentBuf = []byte{}
		}
	}
	return result
}
