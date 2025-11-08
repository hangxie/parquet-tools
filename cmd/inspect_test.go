package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"

	pio "github.com/hangxie/parquet-tools/io"
)

func Test_Inspect_File(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd     InspectCmd
		golden  string
		wantErr bool
	}{
		"good": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet"},
			golden:  "inspect-good-file.json",
			wantErr: false,
		},
		"dict-page": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "dict-page.parquet"},
			golden:  "inspect-dict-page-file.json",
			wantErr: false,
		},
		"row-group": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "row-group.parquet"},
			golden:  "inspect-row-group-file.json",
			wantErr: false,
		},
		"non-existent": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "nonexistent.parquet"},
			wantErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.wantErr {
				t.Parallel()
			}
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			if tc.wantErr {
				require.Error(t, tc.cmd.Run())
			} else {
				tc.golden = "../testdata/golden/" + tc.golden
				stdout, stderr := captureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				expected := loadExpected(t, tc.golden)
				require.Equal(t, expected, stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func Test_Inspect_RowGroup(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd     InspectCmd
		golden  string
		wantErr bool
		errMsg  string
	}{
		"good-rg-0": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0)},
			golden:  "inspect-good-row-group-0.json",
			wantErr: false,
		},
		"row-group-rg-0": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: common.ToPtr(0)},
			golden:  "inspect-row-group-rg-0.json",
			wantErr: false,
		},
		"row-group-rg-1": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: common.ToPtr(1)},
			golden:  "inspect-row-group-rg-1.json",
			wantErr: false,
		},
		"negative-index": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(-1)},
			wantErr: true,
			errMsg:  "row group index -1 out of range",
		},
		"out-of-range": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(999)},
			wantErr: true,
			errMsg:  "row group index 999 out of range",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.wantErr {
				t.Parallel()
			}
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			if tc.wantErr {
				err := tc.cmd.Run()
				require.Error(t, err)
				if tc.errMsg != "" {
					require.Contains(t, err.Error(), tc.errMsg)
				}
			} else {
				tc.golden = "../testdata/golden/" + tc.golden
				stdout, stderr := captureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				expected := loadExpected(t, tc.golden)
				require.Equal(t, expected, stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func Test_Inspect_ColumnChunk(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd     InspectCmd
		golden  string
		wantErr bool
		errMsg  string
	}{
		"good-col-0": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0)},
			golden:  "inspect-good-column-chunk-0.json",
			wantErr: false,
		},
		"dict-page-col-0": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "dict-page.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0)},
			golden:  "inspect-dict-page-column-chunk-0.json",
			wantErr: false,
		},
		"negative-column-index": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(-1)},
			wantErr: true,
			errMsg:  "column chunk index -1 out of range",
		},
		"out-of-range-column": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(999)},
			wantErr: true,
			errMsg:  "column chunk index 999 out of range",
		},
		"column-without-row-group": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", ColumnChunk: common.ToPtr(0)},
			wantErr: true,
			errMsg:  "--column-chunk requires --row-group",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.wantErr {
				t.Parallel()
			}
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			if tc.wantErr {
				err := tc.cmd.Run()
				require.Error(t, err)
				if tc.errMsg != "" {
					require.Contains(t, err.Error(), tc.errMsg)
				}
			} else {
				tc.golden = "../testdata/golden/" + tc.golden
				stdout, stderr := captureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				expected := loadExpected(t, tc.golden)
				require.Equal(t, expected, stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func Test_Inspect_Page(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd     InspectCmd
		golden  string
		wantErr bool
		errMsg  string
	}{
		"good-page-0": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)},
			golden:  "inspect-good-page-0.json",
			wantErr: false,
		},
		"dict-page-page-0": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "dict-page.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)},
			golden:  "inspect-dict-page-page-0.json",
			wantErr: false,
		},
		"row-group-rg1-page-0": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "row-group.parquet", RowGroup: common.ToPtr(1), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(0)},
			golden:  "inspect-row-group-rg1-page-0.json",
			wantErr: false,
		},
		"negative-page-index": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(-1)},
			wantErr: true,
			errMsg:  "page index -1 out of range",
		},
		"out-of-range-page": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), ColumnChunk: common.ToPtr(0), Page: common.ToPtr(999)},
			wantErr: true,
			errMsg:  "page index 999 out of range",
		},
		"page-without-row-group-and-column": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", Page: common.ToPtr(0)},
			wantErr: true,
			errMsg:  "--page requires both --row-group and --column-chunk",
		},
		"page-without-column": {
			cmd:     InspectCmd{ReadOption: rOpt, URI: "good.parquet", RowGroup: common.ToPtr(0), Page: common.ToPtr(0)},
			wantErr: true,
			errMsg:  "--page requires both --row-group and --column-chunk",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.wantErr {
				t.Parallel()
			}
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			if tc.wantErr {
				err := tc.cmd.Run()
				require.Error(t, err)
				if tc.errMsg != "" {
					require.Contains(t, err.Error(), tc.errMsg)
				}
			} else {
				tc.golden = "../testdata/golden/" + tc.golden
				stdout, stderr := captureStdoutStderr(func() {
					require.NoError(t, tc.cmd.Run())
				})
				expected := loadExpected(t, tc.golden)
				require.Equal(t, expected, stdout)
				require.Equal(t, "", stderr)
			}
		})
	}
}

func Test_Inspect_SpecialTypes(t *testing.T) {
	rOpt := pio.ReadOption{}
	testCases := map[string]struct {
		cmd    InspectCmd
		golden string
	}{
		"geospatial": {
			cmd:    InspectCmd{ReadOption: rOpt, URI: "geospatial.parquet", RowGroup: common.ToPtr(0)},
			golden: "inspect-geospatial-row-group-0.json",
		},
		"nil-statistics": {
			cmd:    InspectCmd{ReadOption: rOpt, URI: "nil-statistics.parquet", RowGroup: common.ToPtr(0)},
			golden: "inspect-nil-statistics-row-group-0.json",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tc.cmd.URI = "../testdata/" + tc.cmd.URI
			tc.golden = "../testdata/golden/" + tc.golden
			stdout, stderr := captureStdoutStderr(func() {
				require.NoError(t, tc.cmd.Run())
			})
			expected := loadExpected(t, tc.golden)
			require.Equal(t, expected, stdout)
			require.Equal(t, "", stderr)
		})
	}
}
