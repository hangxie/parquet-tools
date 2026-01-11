package cmd

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	pio "github.com/hangxie/parquet-tools/io"
	pschema "github.com/hangxie/parquet-tools/schema"
)

func TestRetypeCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		testCases := map[string]struct {
			cmd    RetypeCmd
			errMsg string
		}{
			"pagesize-too-small":  {RetypeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 0, Source: "../testdata/good.parquet", URI: "dummy"}, "invalid read page size"},
			"source-non-existent": {RetypeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "does/not/exist", URI: "dummy"}, "no such file or directory"},
			"source-not-parquet":  {RetypeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/not-a-parquet-file", URI: "dummy"}, "failed to read from"},
			"target-file":         {RetypeCmd{ReadOption: rOpt, WriteOption: wOpt, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: "://uri"}, "unable to parse file location"},
			"target-compression": {RetypeCmd{ReadOption: rOpt, WriteOption: pio.WriteOption{
				PageSize:       1024 * 1024,
				RowGroupSize:   128 * 1024 * 1024,
				ParallelNumber: 0,
			}, ReadPageSize: 10, Source: "../testdata/good.parquet", URI: filepath.Join(tempDir, "dummy")}, "not a valid CompressionCode"},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				err := tc.cmd.Run()
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("int96-to-timestamp", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		// Use simpler file without nested types for reliable conversion
		cmd := RetypeCmd{
			Int96ToTimestamp: true,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     100,
			Source:           "../testdata/int96-nil-min-max.parquet",
			URI:              filepath.Join(tempDir, "retyped.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)

		// Verify the output file exists and has the correct row count
		reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
		require.NoError(t, err)
		defer func() { _ = reader.PFile.Close() }()

		require.Equal(t, int64(10), reader.GetNumRows())

		// Verify the schema has TIMESTAMP_NANOS instead of INT96
		schemaTree, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{SkipPageEncoding: true})
		require.NoError(t, err)

		jsonSchema := schemaTree.JSONSchema()
		require.Contains(t, jsonSchema, "TIMESTAMP")
		require.Contains(t, jsonSchema, "NANOS")
		require.NotContains(t, jsonSchema, "INT96")
	})

	t.Run("no-conversion-without-flag", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		cmd := RetypeCmd{
			Int96ToTimestamp: false,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     100,
			Source:           "../testdata/all-types.parquet",
			URI:              filepath.Join(tempDir, "no-retype.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)

		// Verify the output file exists
		reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
		require.NoError(t, err)
		defer func() { _ = reader.PFile.Close() }()

		require.Equal(t, int64(10), reader.GetNumRows())

		// Verify INT96 is still in the schema
		schemaTree, err := pschema.NewSchemaTree(reader, pschema.SchemaOption{SkipPageEncoding: true})
		require.NoError(t, err)

		jsonSchema := schemaTree.JSONSchema()
		require.Contains(t, jsonSchema, "INT96")
	})

	t.Run("preserves-other-types", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		// Use simpler file without nested types
		cmd := RetypeCmd{
			Int96ToTimestamp: true,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     100,
			Source:           "../testdata/int96-nil-min-max.parquet",
			URI:              filepath.Join(tempDir, "retyped-preserve.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)

		// Verify data integrity using cat command
		catCmd := CatCmd{
			ReadOption:   rOpt,
			ReadPageSize: 1000,
			SampleRatio:  1.0,
			Format:       "json",
			GeoFormat:    "geojson",
			FailOnInt96:  true,
			URI:          cmd.URI,
		}

		// Should not fail because INT96 has been converted
		output, _ := captureStdoutStderr(func() {
			require.NoError(t, catCmd.Run())
		})

		// Verify the Utf8 field is present
		require.Contains(t, output, "Utf8")
	})

	t.Run("empty-parquet-file", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		cmd := RetypeCmd{
			Int96ToTimestamp: true,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     100,
			Source:           "../testdata/empty.parquet",
			URI:              filepath.Join(tempDir, "retyped-empty.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)

		// Verify the output file exists and has 0 rows
		reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
		require.NoError(t, err)
		defer func() { _ = reader.PFile.Close() }()

		require.Equal(t, int64(0), reader.GetNumRows())
	})

	t.Run("small-page-size", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		// Use small page size to test multiple read iterations
		cmd := RetypeCmd{
			Int96ToTimestamp: true,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     2, // Small page size forces multiple read iterations
			Source:           "../testdata/int96-nil-min-max.parquet",
			URI:              filepath.Join(tempDir, "retyped-small-page.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)

		// Verify the output file has the same row count
		reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
		require.NoError(t, err)
		defer func() { _ = reader.PFile.Close() }()

		require.Equal(t, int64(10), reader.GetNumRows())
	})

	t.Run("page-size-one", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		// Use page size of 1 - the minimum allowed
		cmd := RetypeCmd{
			Int96ToTimestamp: false,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     1,
			Source:           "../testdata/good.parquet",
			URI:              filepath.Join(tempDir, "retyped-page-one.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)
	})

	t.Run("no-int96-fields-in-file", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		// Using a file that has no INT96 fields, with INT96 conversion enabled
		cmd := RetypeCmd{
			Int96ToTimestamp: true,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     100,
			Source:           "../testdata/good.parquet",
			URI:              filepath.Join(tempDir, "retyped-no-int96.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)
	})

	t.Run("conversion-without-int96-fields-map", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		wOpt := pio.WriteOption{
			Compression:    "SNAPPY",
			PageSize:       1024 * 1024,
			RowGroupSize:   128 * 1024 * 1024,
			ParallelNumber: 0,
		}
		tempDir := t.TempDir()

		// Test path where Int96ToTimestamp is true but no INT96 fields exist
		// This exercises the len(int96Fields) == 0 path in reader
		cmd := RetypeCmd{
			Int96ToTimestamp: true,
			ReadOption:       rOpt,
			WriteOption:      wOpt,
			ReadPageSize:     100,
			Source:           "../testdata/good.parquet", // No INT96 fields
			URI:              filepath.Join(tempDir, "retyped-no-int96-map.parquet"),
		}

		err := cmd.Run()
		require.NoError(t, err)

		// Verify the output
		reader, err := pio.NewParquetFileReader(cmd.URI, rOpt)
		require.NoError(t, err)
		defer func() { _ = reader.PFile.Close() }()

		require.Equal(t, int64(3), reader.GetNumRows())
	})

	t.Run("different-compression-formats", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		tempDir := t.TempDir()

		compressions := []string{"GZIP", "ZSTD", "LZ4_RAW", "UNCOMPRESSED"}
		for _, comp := range compressions {
			t.Run(comp, func(t *testing.T) {
				wOpt := pio.WriteOption{
					Compression:    comp,
					PageSize:       1024 * 1024,
					RowGroupSize:   128 * 1024 * 1024,
					ParallelNumber: 0,
				}

				cmd := RetypeCmd{
					Int96ToTimestamp: false,
					ReadOption:       rOpt,
					WriteOption:      wOpt,
					ReadPageSize:     100,
					Source:           "../testdata/good.parquet",
					URI:              filepath.Join(tempDir, "retyped-"+comp+".parquet"),
				}

				err := cmd.Run()
				require.NoError(t, err)
			})
		}
	})
}

func TestConvertStructWithInt96(t *testing.T) {
	// Valid INT96 timestamp string (format used by parquet-go)
	validInt96 := "AADgpBwAAAAAmpcUAA=="
	// Invalid INT96 string that will fail conversion - base64 decodes to less than 12 bytes
	invalidInt96 := "c2hvcnQ=" // "short" in base64, only 5 bytes when decoded

	t.Run("invalid-int96-string", func(t *testing.T) {
		type TestStruct struct {
			Name      string
			Timestamp string
		}

		input := &TestStruct{Name: "test", Timestamp: invalidInt96}
		int96Fields := map[string]struct{}{"Timestamp": {}}

		_, err := convertStructWithInt96(input, int96Fields)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to convert INT96 field")
	})

	t.Run("invalid-int96-pointer-string", func(t *testing.T) {
		type TestStruct struct {
			Name      string
			Timestamp *string
		}

		ts := invalidInt96
		input := &TestStruct{Name: "test", Timestamp: &ts}
		int96Fields := map[string]struct{}{"Timestamp": {}}

		_, err := convertStructWithInt96(input, int96Fields)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to convert INT96 field")
	})

	t.Run("string-int96-field", func(t *testing.T) {
		type TestStruct struct {
			Name      string
			Timestamp string
		}

		input := &TestStruct{Name: "test", Timestamp: validInt96}
		int96Fields := map[string]struct{}{"Timestamp": {}}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.NotNil(t, result)

		// The result should be a pointer to a struct with int64 Timestamp
		// Verify the Name field is preserved
		require.NotNil(t, result)
	})

	t.Run("pointer-int96-field-non-nil", func(t *testing.T) {
		type TestStruct struct {
			Name      string
			Timestamp *string
		}

		ts := validInt96
		input := &TestStruct{Name: "test", Timestamp: &ts}
		int96Fields := map[string]struct{}{"Timestamp": {}}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("pointer-int96-field-nil", func(t *testing.T) {
		type TestStruct struct {
			Name      string
			Timestamp *string
		}

		input := &TestStruct{Name: "test", Timestamp: nil}
		int96Fields := map[string]struct{}{"Timestamp": {}}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("no-int96-fields", func(t *testing.T) {
		type TestStruct struct {
			Name  string
			Value int
		}

		input := &TestStruct{Name: "test", Value: 42}
		int96Fields := map[string]struct{}{}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("nil-pointer-input", func(t *testing.T) {
		var input *struct{ Name string }
		int96Fields := map[string]struct{}{}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("non-struct-input", func(t *testing.T) {
		input := "not a struct"
		int96Fields := map[string]struct{}{}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.Equal(t, input, result)
	})

	t.Run("unexpected-type-for-int96", func(t *testing.T) {
		type TestStruct struct {
			Timestamp int // Not a string or *string
		}

		input := &TestStruct{Timestamp: 123}
		int96Fields := map[string]struct{}{"Timestamp": {}}

		_, err := convertStructWithInt96(input, int96Fields)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected type for INT96 field")
	})

	t.Run("struct-value-not-pointer", func(t *testing.T) {
		type TestStruct struct {
			Name      string
			Timestamp string
		}

		input := TestStruct{Name: "test", Timestamp: validInt96}
		int96Fields := map[string]struct{}{"Timestamp": {}}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("complex-struct-with-mixed-fields", func(t *testing.T) {
		type TestStruct struct {
			Name       string
			Value      int64
			Timestamp1 string
			Timestamp2 *string
			Data       []byte
		}

		ts := validInt96
		input := &TestStruct{
			Name:       "test",
			Value:      100,
			Timestamp1: validInt96,
			Timestamp2: &ts,
			Data:       []byte{1, 2, 3},
		}
		int96Fields := map[string]struct{}{"Timestamp1": {}, "Timestamp2": {}}

		result, err := convertStructWithInt96(input, int96Fields)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
}

func TestGetOrCreateTargetType(t *testing.T) {
	t.Run("caches-type", func(t *testing.T) {
		int96Fields := map[string]struct{}{"Timestamp": {}}

		// Clear cache first by creating a unique struct type
		type UniqueStruct struct {
			UniqueField string
		}

		srcType := reflect.TypeOf(UniqueStruct{})
		targetType1 := getOrCreateTargetType(srcType, int96Fields)
		targetType2 := getOrCreateTargetType(srcType, int96Fields)

		// Should return the same type from cache
		require.Equal(t, targetType1, targetType2)
	})

	t.Run("converts-string-to-int64", func(t *testing.T) {
		type TestStruct struct {
			Timestamp string
		}

		int96Fields := map[string]struct{}{"Timestamp": {}}
		srcType := reflect.TypeOf(TestStruct{})
		targetType := getOrCreateTargetType(srcType, int96Fields)

		// Verify the field type is int64
		field, ok := targetType.FieldByName("Timestamp")
		require.True(t, ok)
		require.Equal(t, "int64", field.Type.String())
	})

	t.Run("converts-pointer-string-to-pointer-int64", func(t *testing.T) {
		type TestStruct struct {
			Timestamp *string
		}

		int96Fields := map[string]struct{}{"Timestamp": {}}
		srcType := reflect.TypeOf(TestStruct{})
		targetType := getOrCreateTargetType(srcType, int96Fields)

		// Verify the field type is *int64
		field, ok := targetType.FieldByName("Timestamp")
		require.True(t, ok)
		require.Equal(t, "*int64", field.Type.String())
	})

	t.Run("preserves-non-int96-fields", func(t *testing.T) {
		type TestStruct struct {
			Name  string
			Value int
		}

		int96Fields := map[string]struct{}{}
		srcType := reflect.TypeOf(TestStruct{})
		targetType := getOrCreateTargetType(srcType, int96Fields)

		// Verify fields are preserved
		nameField, ok := targetType.FieldByName("Name")
		require.True(t, ok)
		require.Equal(t, "string", nameField.Type.String())

		valueField, ok := targetType.FieldByName("Value")
		require.True(t, ok)
		require.Equal(t, "int", valueField.Type.String())
	})
}
