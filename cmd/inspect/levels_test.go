package inspect

import (
	"strings"
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
)

func TestInspectRowGroupOutOfRange(t *testing.T) {
	parquetReader := &reader.ParquetReader{Footer: &parquet.FileMetaData{}}
	err := (Cmd{}).inspectRowGroup(parquetReader, 0, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("inspectRowGroup() error = %v, want out-of-range error", err)
	}
}
