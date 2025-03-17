package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

// this for unit test only
func captureStdoutStderr(f func()) (string, string) {
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

// this for unit test only
func loadExpected(t *testing.T, fileName string) string {
	buf, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal("cannot load golden file:", fileName, "because of:", err.Error())
	}
	if !strings.HasSuffix(fileName, ".json") && !strings.HasSuffix(fileName, ".jsonl") {
		return string(buf)
	}

	// JSON and JSONL golden files are formatted by jq
	var result string
	currentBuf := []byte{}
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
