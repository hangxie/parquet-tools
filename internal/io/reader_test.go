package io

import (
	"encoding/base64"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_NewParquetFileReader(t *testing.T) {
	rOpt := ReadOption{}
	s3URL := "s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0"
	gcsURL := "gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
	azblobURL := "wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet"
	httpURL := "https://dpla-provider-export.s3.amazonaws.com/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet"
	testCases := map[string]struct {
		uri    string
		option ReadOption
		errMsg string
	}{
		"invalid-uri":            {"://uri", rOpt, "unable to parse file location"},
		"invalid-scheme":         {"invalid-scheme://something", rOpt, "unknown location scheme"},
		"local-file-not-found":   {"file://path/to/file", rOpt, "no such file or directory"},
		"local-file-not-parquet": {"../../testdata/not-a-parquet-file", rOpt, "invalid argument"},
		"local-file-good":        {"../../testdata/good.parquet", rOpt, ""},
		"s3-not-found":           {"s3://bucket-does-not-exist", rOpt, "not found"},
		"s3-good":                {s3URL, ReadOption{Anonymous: true}, ""},
		"s3-wrong-version":       {s3URL, ReadOption{ObjectVersion: "random-version-id", Anonymous: true}, "https response error StatusCode: 400"},
		"gcs-no-permission":      {gcsURL, rOpt, "failed to create GCS client"},
		"gcs-good":               {gcsURL, ReadOption{Anonymous: true}, ""},
		"azblob-no-permission":   {azblobURL, rOpt, "Server failed to authenticate the request"},
		"azblob-good":            {azblobURL, ReadOption{Anonymous: true}, ""},
		"http-bad-url":           {"https://.../", rOpt, "no such host"},
		"http-no-range-support":  {"https://www.google.com/", rOpt, "does not support range"},
		"http-good":              {httpURL, rOpt, ""},
		"hdfs-failed":            {"hdfs://localhost:1/temp/good.parquet", rOpt, "connection refused"},
	}

	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null")
	t.Setenv("AZURE_STORAGE_ACCESS_KEY", base64.StdEncoding.EncodeToString(uuid.New().NodeID()))
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := NewParquetFileReader(tc.uri, tc.option)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}
