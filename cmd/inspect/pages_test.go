package inspect

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
)

func TestConvertIndexPageHeader(t *testing.T) {
	page := (Cmd{}).convertPageHeaderInfo(reader.PageHeaderInfo{PageType: parquet.PageType_INDEX_PAGE}, nil)
	if page.Note != "Index page (column index)" {
		t.Fatalf("convertPageHeaderInfo() note = %q", page.Note)
	}
}
