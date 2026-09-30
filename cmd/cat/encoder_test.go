package cat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
)

func TestEncoderValueMode(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=Amount, type=INT64, convertedtype=DECIMAL, precision=18, scale=2"},{"Tag":"name=Unknown, type=INT32, repetitiontype=OPTIONAL, logicaltype=UNKNOWN"}]}`)
	require.NoError(t, err)
	for _, tc := range []struct{ mode, geo, want string }{
		{"", "geojson", `{"Amount":1.23,"Unknown":null}`},
		{"interpreted", "geojson", `{"Amount":1.23,"Unknown":null}`},
		{"raw", "geojson", `{"Amount":123,"Unknown":7}`},
		{"raw", "hybrid", `{"Amount":123,"Unknown":7}`},
	} {
		t.Run(tc.mode+tc.geo, func(t *testing.T) {
			input := make(chan any, 1)
			input <- struct {
				Amount  int64
				Unknown int32
			}{123, 7}
			close(input)
			output := make(chan string, 1)
			cmd := Cmd{Format: "json", ValueMode: tc.mode, GeoFormat: tc.geo}
			require.NoError(t, cmd.encoder(context.Background(), input, output, handler, nil, map[string]struct{}{"Unknown": {}}))
			require.JSONEq(t, tc.want, <-output)
		})
	}
}

func TestRawValueModeLegacyList(t *testing.T) {
	interpreted := Cmd{URI: "../../testdata/old-style-list.parquet", Format: "json", ReadPageSize: 10, SampleRatio: 1}
	expected := testutils.CommandStdout(t, interpreted)
	raw := interpreted
	raw.ValueMode = "raw"
	require.Equal(t, expected, testutils.CommandStdout(t, raw))
}

func TestRawValueModeFixtures(t *testing.T) {
	for _, fixture := range []string{"all-types.parquet", "geospatial.parquet", "retype.parquet"} {
		t.Run(fixture, func(t *testing.T) {
			cmd := Cmd{URI: "../../testdata/" + fixture, Format: "json", ReadPageSize: 2, SampleRatio: 1, ValueMode: "raw", Concurrent: true}
			var rows []map[string]any
			require.NoError(t, json.Unmarshal([]byte(testutils.CommandStdout(t, cmd)), &rows))
			require.NotEmpty(t, rows)
			if fixture == "geospatial.parquet" {
				for _, row := range rows {
					for _, column := range []string{"Geography", "Geometry"} {
						value, ok := row[column].(string)
						require.True(t, ok)
						data, err := base64.StdEncoding.DecodeString(value)
						require.NoError(t, err)
						require.NotEmpty(t, data)
					}
				}
			}
		})
	}
}

func TestInvalidValueMode(t *testing.T) {
	require.ErrorContains(t, (Cmd{ValueMode: "auto"}).Run(context.Background()), "invalid value mode")
}
