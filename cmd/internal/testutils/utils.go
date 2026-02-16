package testutils

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

var stdCaptureMutex sync.Mutex

// HasSameSchema compares the logical schema of two parquet files using structural
// tree comparison. An optional CompareOption can be provided to control which
// writer directives (encoding, compression, etc.) are included in the comparison;
// by default all directives are ignored.
func HasSameSchema(file1, file2 string, opts ...pschema.CompareOption) bool {
	var option pschema.CompareOption
	if len(opts) > 0 {
		option = opts[0]
	}
	buildTree := func(file string) *pschema.SchemaNode {
		pr, err := pio.NewParquetFileReader(file, pio.ReadOption{})
		if err != nil {
			return nil
		}
		defer func() { _ = pr.PFile.Close() }()
		tree, err := pschema.NewSchemaTree(pr, pschema.SchemaOption{})
		if err != nil {
			return nil
		}
		return tree
	}

	tree1 := buildTree(file1)
	tree2 := buildTree(file2)
	if tree1 == nil || tree2 == nil {
		return false
	}
	return tree1.IsCompatible(tree2, option)
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
