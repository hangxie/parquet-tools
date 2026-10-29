package retype

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/types"
	"github.com/hangxie/parquet-go/v3/writer"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hangxie/parquet-tools/cmd/cat"
	"github.com/hangxie/parquet-tools/cmd/internal/testutils"
	"github.com/hangxie/parquet-tools/cmd/schema"

	pio "github.com/hangxie/parquet-tools/io"
)

func TestCmd(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		rOpt := pio.ReadOption{}

		testCases := map[string]struct {
			cmd    Cmd
			errMsg string
		}{
			"pagesize-too-small": {
				Cmd{ReadOption: rOpt, ReadPageSize: 0, Source: "../../testdata/good.parquet", URI: "dummy"},
				"invalid read page size",
			},
			"source-non-existent": {
				Cmd{ReadOption: rOpt, ReadPageSize: 10, Source: "does/not/exist", URI: "dummy"},
				"no such file or directory",
			},
			"source-not-parquet": {
				Cmd{ReadOption: rOpt, ReadPageSize: 10, Source: "../../testdata/not-a-parquet-file", URI: "dummy"},
				"failed to read from",
			},
			"target-file": {
				Cmd{ReadOption: rOpt, ReadPageSize: 10, Source: "../../testdata/good.parquet", URI: "://uri"},
				"unable to parse file location",
			},
			"source-schema-error": {
				Cmd{ReadOption: rOpt, ReadPageSize: 10, Source: "../../testdata/ARROW-GH-41317.parquet", URI: "dummy"},
				"failed to build encoding map",
			},
			"field-delimiter": {
				Cmd{ReadOption: rOpt, ReadPageSize: 10, FieldDelimiter: "::", Source: "../../testdata/good.parquet", URI: "dummy"},
				"field delimiter must be a single character",
			},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				err := tc.cmd.Run(context.Background())
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("good", func(t *testing.T) {
		rOpt := pio.ReadOption{}
		tempDir := t.TempDir()
		resultFile := filepath.Join(tempDir, "retyped.parquet")
		testCases := map[string]struct {
			cmd          Cmd
			goldenSchema string
			goldenData   string
		}{
			"int96-to-timestamp": {
				cmd: Cmd{
					Int96ToTimestamp: true,
					ReadOption:       rOpt,
					ReadPageSize:     100,
					Source:           "../../testdata/retype.parquet",
					URI:              resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-schema-int96-to-timestamp.json",
				goldenData:   "../../testdata/golden/retype-data.json",
			},
			"bson-to-string": {
				cmd: Cmd{
					BsonToString: true,
					ReadOption:   rOpt,
					ReadPageSize: 100,
					Source:       "../../testdata/retype.parquet",
					URI:          resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-schema-bson-to-string.json",
				goldenData:   "../../testdata/golden/retype-data-bson-to-string.json",
			},
			"float16-to-float32": {
				cmd: Cmd{
					Float16ToFloat32: true,
					ReadOption:       rOpt,
					ReadPageSize:     100,
					Source:           "../../testdata/retype.parquet",
					URI:              resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-schema-float16-to-float32.json",
				goldenData:   "../../testdata/golden/retype-data.json",
			},
			"json-to-string": {
				cmd: Cmd{
					JsonToString: true,
					ReadOption:   rOpt,
					ReadPageSize: 100,
					Source:       "../../testdata/retype.parquet",
					URI:          resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-schema-json-to-string.json",
				goldenData:   "../../testdata/golden/retype-data.json",
			},
			"no-retype": {
				cmd: Cmd{
					Int96ToTimestamp: false,
					ReadOption:       rOpt,
					ReadPageSize:     100,
					Source:           "../../testdata/retype.parquet",
					URI:              resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-schema.json",
				goldenData:   "../../testdata/golden/retype-data.json",
			},
			"variant-to-string": {
				cmd: Cmd{
					VariantToString: true,
					ReadOption:      rOpt,
					ReadPageSize:    100,
					Source:          "../../testdata/all-types.parquet",
					URI:             resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-all-types-variant-to-string-schema.json",
				goldenData:   "../../testdata/golden/retype-all-types-variant-to-string-data.json",
			},
			"repeated-to-list": {
				cmd: Cmd{
					RepeatedToList: true,
					ReadOption:     rOpt,
					ReadPageSize:   100,
					Source:         "../../testdata/all-types.parquet",
					URI:            resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-all-types-repeated-to-list-schema.json",
				goldenData:   "../../testdata/golden/retype-all-types-repeated-to-list-data.json",
			},
			"uuid-to-string": {
				cmd: Cmd{
					UuidToString: true,
					ReadOption:   rOpt,
					ReadPageSize: 100,
					Source:       "../../testdata/all-types.parquet",
					URI:          resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-all-types-uuid-to-string-schema.json",
				goldenData:   "../../testdata/golden/retype-all-types-uuid-to-string-data.json",
			},
			"geo-to-binary": {
				cmd: Cmd{
					GeoToBinary:  true,
					ReadOption:   rOpt,
					ReadPageSize: 100,
					Source:       "../../testdata/geospatial.parquet",
					URI:          resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-geospatial-geo-to-binary-schema.json",
				goldenData:   "../../testdata/golden/retype-geospatial-geo-to-binary-data.json",
			},
			// Rewriting the REPEATED double column moves every non-finite value
			// into a new LIST group; the values themselves must not change.
			"non-finite-repeated-to-list": {
				cmd: Cmd{
					RepeatedToList: true,
					ReadOption:     rOpt,
					ReadPageSize:   100,
					Source:         "../../testdata/non-finite.parquet",
					URI:            resultFile,
				},
				goldenSchema: "../../testdata/golden/retype-non-finite-repeated-to-list-schema.json",
				goldenData:   "../../testdata/golden/retype-non-finite-repeated-to-list-data.json",
			},
		}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				err := tc.cmd.Run(context.Background())
				require.NoError(t, err)

				stdout, stderr := testutils.CaptureStdoutStderr(func() {
					cmd := cat.Cmd{
						ReadOption:   rOpt,
						ReadPageSize: 1000,
						SampleRatio:  1.0,
						Format:       "json",
						URI:          resultFile,
					}
					require.NoError(t, cmd.Run(context.Background()))
				})
				require.Equal(t, testutils.LoadExpected(t, tc.goldenData), stdout)
				require.Equal(t, "", stderr)

				stdout, stderr = testutils.CaptureStdoutStderr(func() {
					cmd := schema.Cmd{
						ReadOption: rOpt,
						Format:     "json",
						URI:        resultFile,
					}
					require.NoError(t, cmd.Run(context.Background()))
				})
				require.Equal(t, testutils.LoadExpected(t, tc.goldenSchema), stdout)
				require.Equal(t, "", stderr)
			})
		}
	})
}

var (
	retypeEncryptionFooterKey = new("MDEyMzQ1Njc4OTAxMjM0NQ==")
	retypeEncryptionColumnKey = "MTIzNDU2Nzg5MDEyMzQ1MA=="
)

func TestCmdEncryption(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "good.parquet")

	testCases := []struct {
		name        string
		writeOption pio.WriteOption
		readOption  pio.ReadOption
		footerMagic string
	}{
		{
			name: "encrypted-footer",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  retypeEncryptionFooterKey,
			},
			readOption:  pio.ReadOption{FooterKey: retypeEncryptionFooterKey},
			footerMagic: "PARE",
		},
		{
			name: "encrypted-footer-column-keys",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  retypeEncryptionFooterKey,
				WriterColumnKeys: []string{"shoe_name=" + retypeEncryptionColumnKey},
			},
			readOption: pio.ReadOption{
				FooterKey:  retypeEncryptionFooterKey,
				ColumnKeys: []string{"shoe_name=" + retypeEncryptionColumnKey},
			},
			footerMagic: "PARE",
		},
		{
			name: "plaintext-footer-column-keys",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  retypeEncryptionFooterKey,
				WriterColumnKeys: []string{"shoe_name=" + retypeEncryptionColumnKey},
				PlaintextFooter:  true,
			},
			readOption: pio.ReadOption{
				FooterKey:  retypeEncryptionFooterKey,
				ColumnKeys: []string{"shoe_name=" + retypeEncryptionColumnKey},
			},
			footerMagic: "PAR1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			uri := filepath.Join(tempDir, tc.name+".parquet")
			cmd := Cmd{
				ReadOption:   pio.ReadOption{},
				WriteOption:  tc.writeOption,
				ReadPageSize: 10,
				Source:       source,
				URI:          uri,
			}
			require.NoError(t, cmd.Run(context.Background()))
			require.Equal(t, tc.footerMagic, testutils.ParquetFooterMagic(t, uri))

			reader, err := pio.NewParquetFileReader(context.Background(), uri, tc.readOption)
			require.NoError(t, err)
			require.Equal(t, int64(3), reader.GetNumRows())
			_ = reader.PFile.Close()
		})
	}
}

func TestCmdEncryptionErrors(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "good.parquet")

	testCases := []struct {
		name        string
		writeOption pio.WriteOption
		errMsg      string
	}{
		{
			name: "missing-footer-key",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterColumnKeys: []string{"shoe_name=" + retypeEncryptionColumnKey},
			},
			errMsg: "--writer-footer-key is required",
		},
		{
			name: "bad-base64",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  new("not base64"),
			},
			errMsg: "invalid base64 writer footer key",
		},
		{
			name: "wrong-key-size",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  new("MTIzNDU="),
			},
			errMsg: "writer footer key must be 16, 24, or 32 bytes",
		},
		{
			name: "missing-column-key-path",
			writeOption: pio.WriteOption{
				CompressionCodec: "SNAPPY",
				DataPageVersion:  2,
				PageSize:         1024 * 1024,
				RowGroupSize:     128 * 1024 * 1024,
				WriterFooterKey:  retypeEncryptionFooterKey,
				WriterColumnKeys: []string{"missing=" + retypeEncryptionColumnKey},
			},
			errMsg: "writer column key path [missing] not found in schema",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			cmd := Cmd{
				ReadOption:   pio.ReadOption{},
				WriteOption:  tc.writeOption,
				ReadPageSize: 10,
				Source:       source,
				URI:          filepath.Join(tempDir, tc.name+".parquet"),
			}
			err := cmd.Run(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestConverter(t *testing.T) {
	validInt96 := "AADgpBwAAAAAmpcUAA=="
	invalidInt96 := "c2hvcnQ="

	t.Run("int96-conversion", func(t *testing.T) {
		t.Run("invalid-int96-string", func(t *testing.T) {
			type TestStruct struct {
				Name      string
				Timestamp string
			}

			input := &TestStruct{Name: "test", Timestamp: invalidInt96}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

			_, err := conv.Convert(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to convert")
		})

		t.Run("invalid-int96-pointer-string", func(t *testing.T) {
			type TestStruct struct {
				Name      string
				Timestamp *string
			}

			ts := invalidInt96
			input := &TestStruct{Name: "test", Timestamp: &ts}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

			_, err := conv.Convert(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to convert")
		})

		t.Run("string-int96-field", func(t *testing.T) {
			type TestStruct struct {
				Name      string
				Timestamp string
			}

			input := &TestStruct{Name: "test", Timestamp: validInt96}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})

		t.Run("pointer-int96-field-non-nil", func(t *testing.T) {
			type TestStruct struct {
				Name      string
				Timestamp *string
			}

			ts := validInt96
			input := &TestStruct{Name: "test", Timestamp: &ts}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})

		t.Run("pointer-int96-field-nil", func(t *testing.T) {
			type TestStruct struct {
				Name      string
				Timestamp *string
			}

			input := &TestStruct{Name: "test", Timestamp: nil}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})

		t.Run("no-matching-fields", func(t *testing.T) {
			type TestStruct struct {
				Name  string
				Value int
			}

			input := &TestStruct{Name: "test", Value: 42}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})

		t.Run("nil-pointer-input", func(t *testing.T) {
			var input *struct{ Name string }
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.Nil(t, result)
		})

		t.Run("non-struct-input", func(t *testing.T) {
			input := "not a struct"
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.Equal(t, input, result)
		})

		t.Run("unexpected-type-for-int96", func(t *testing.T) {
			type TestStruct struct {
				Timestamp int
			}

			input := &TestStruct{Timestamp: 123}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

			_, err := conv.Convert(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "expected string for INT96")
		})

		t.Run("struct-value-not-pointer", func(t *testing.T) {
			type TestStruct struct {
				Name      string
				Timestamp string
			}

			input := TestStruct{Name: "test", Timestamp: validInt96}
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

			result, err := conv.Convert(input)
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
			rule := RuleRegistry[RuleInt96ToTimestamp]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp1": {}, "Timestamp2": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})
	})

	t.Run("bson-conversion", func(t *testing.T) {
		bsonData, err := bson.Marshal(bson.M{"key": "value"})
		require.NoError(t, err)
		validBson := string(bsonData)
		invalidBson := "not valid bson"

		t.Run("valid-bson-string", func(t *testing.T) {
			type TestStruct struct {
				Name string
				Data string
			}

			input := &TestStruct{Name: "test", Data: validBson}
			rule := RuleRegistry[RuleBsonToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)

			resultVal := reflect.ValueOf(result).Elem()
			dataField := resultVal.FieldByName("Data")
			require.Contains(t, dataField.String(), `"key"`)
			require.Contains(t, dataField.String(), `"value"`)
		})

		t.Run("invalid-bson-string", func(t *testing.T) {
			type TestStruct struct {
				Name string
				Data string
			}

			input := &TestStruct{Name: "test", Data: invalidBson}
			rule := RuleRegistry[RuleBsonToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			_, err := conv.Convert(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to convert")
		})

		t.Run("pointer-bson-field-non-nil", func(t *testing.T) {
			type TestStruct struct {
				Name string
				Data *string
			}

			data := validBson
			input := &TestStruct{Name: "test", Data: &data}
			rule := RuleRegistry[RuleBsonToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})

		t.Run("pointer-bson-field-nil", func(t *testing.T) {
			type TestStruct struct {
				Name string
				Data *string
			}

			input := &TestStruct{Name: "test", Data: nil}
			rule := RuleRegistry[RuleBsonToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})

		t.Run("no-bson-fields", func(t *testing.T) {
			type TestStruct struct {
				Name  string
				Value int
			}

			input := &TestStruct{Name: "test", Value: 42}
			rule := RuleRegistry[RuleBsonToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})

		t.Run("unexpected-type-for-bson", func(t *testing.T) {
			type TestStruct struct {
				Data int
			}

			input := &TestStruct{Data: 123}
			rule := RuleRegistry[RuleBsonToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			_, err := conv.Convert(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "expected string for BSON")
		})
	})

	t.Run("float16-conversion", func(t *testing.T) {
		t.Run("valid-float16", func(t *testing.T) {
			validFloat16 := string([]byte{0x00, 0x3C})

			type TestStruct struct {
				Value string
			}

			input := &TestStruct{Value: validFloat16}
			rule := RuleRegistry[RuleFloat16ToFloat32]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Value": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)

			resultVal := reflect.ValueOf(result).Elem()
			valueField := resultVal.FieldByName("Value")
			require.InDelta(t, float32(1.0), valueField.Interface().(float32), 0.001)
		})

		t.Run("invalid-float16-length", func(t *testing.T) {
			invalidFloat16 := "x"

			type TestStruct struct {
				Value string
			}

			input := &TestStruct{Value: invalidFloat16}
			rule := RuleRegistry[RuleFloat16ToFloat32]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Value": {}}}, nil)

			_, err := conv.Convert(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "float16 requires 2 bytes")
		})
	})

	t.Run("uuid-conversion", func(t *testing.T) {
		t.Run("valid-uuid", func(t *testing.T) {
			// 16 bytes of zeros
			validUuid := string(make([]byte, 16))

			type TestStruct struct {
				Value string
			}

			input := &TestStruct{Value: validUuid}
			rule := RuleRegistry[RuleUuidToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Value": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)

			resultVal := reflect.ValueOf(result).Elem()
			valueField := resultVal.FieldByName("Value")
			require.Equal(t, "00000000-0000-0000-0000-000000000000", valueField.String())
		})

		t.Run("invalid-uuid-length", func(t *testing.T) {
			invalidUuid := "too short"

			type TestStruct struct {
				Value string
			}

			input := &TestStruct{Value: invalidUuid}
			rule := RuleRegistry[RuleUuidToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Value": {}}}, nil)

			_, err := conv.Convert(input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "UUID requires 16 bytes")
		})
	})

	t.Run("variant-conversion", func(t *testing.T) {
		t.Run("map-input", func(t *testing.T) {
			type TestStruct struct {
				Data any
			}
			inputData := map[string]any{"key": "value", "num": 123}
			input := &TestStruct{Data: inputData}
			rule := RuleRegistry[RuleVariantToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)

			resultVal := reflect.ValueOf(result).Elem()
			dataField := resultVal.FieldByName("Data")
			require.Equal(t, "string", dataField.Type().String())

			// Verify JSON content
			var decoded map[string]any
			err = json.Unmarshal([]byte(dataField.String()), &decoded)
			require.NoError(t, err)
			require.Equal(t, "value", decoded["key"])
			require.Equal(t, float64(123), decoded["num"]) // JSON numbers are float64
		})

		t.Run("slice-input", func(t *testing.T) {
			type TestStruct struct {
				Data any
			}
			inputData := []any{"item1", 2}
			input := &TestStruct{Data: inputData}
			rule := RuleRegistry[RuleVariantToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)
			require.NotNil(t, result)

			resultVal := reflect.ValueOf(result).Elem()
			dataField := resultVal.FieldByName("Data")

			var decoded []any
			err = json.Unmarshal([]byte(dataField.String()), &decoded)
			require.NoError(t, err)
			require.Equal(t, "item1", decoded[0])
			require.Equal(t, float64(2), decoded[1])
		})

		t.Run("primitive-input", func(t *testing.T) {
			type TestStruct struct {
				Data any
			}
			input := &TestStruct{Data: "simple string"}
			rule := RuleRegistry[RuleVariantToString]
			conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

			result, err := conv.Convert(input)
			require.NoError(t, err)

			resultVal := reflect.ValueOf(result).Elem()
			dataField := resultVal.FieldByName("Data")
			require.Equal(t, `"simple string"`, dataField.String())
		})
	})

	t.Run("no-rules", func(t *testing.T) {
		type TestStruct struct {
			Name  string
			Value int
		}

		input := &TestStruct{Name: "test", Value: 42}
		conv := NewConverter(nil, nil, nil)

		result, err := conv.Convert(input)
		require.NoError(t, err)
		require.Equal(t, input, result)
	})

	t.Run("schema-only-rule", func(t *testing.T) {
		type TestStruct struct {
			Json string
		}

		input := &TestStruct{Json: `{"key":"value"}`}
		rule := RuleRegistry[RuleJsonToString]
		conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Json": {}}}, nil)

		result, err := conv.Convert(input)
		require.NoError(t, err)
		require.Equal(t, input, result)
	})
}

func TestGetOrCreateTargetType(t *testing.T) {
	t.Run("converts-string-to-int64", func(t *testing.T) {
		type TestStruct struct {
			Timestamp string
		}

		rule := RuleRegistry[RuleInt96ToTimestamp]
		conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

		srcType := reflect.TypeFor[TestStruct]()
		targetType := conv.targetType(srcType, "")

		field, ok := targetType.FieldByName("Timestamp")
		require.True(t, ok)
		require.Equal(t, "int64", field.Type.String())
	})

	t.Run("converts-pointer-string-to-pointer-int64", func(t *testing.T) {
		type TestStruct struct {
			Timestamp *string
		}

		rule := RuleRegistry[RuleInt96ToTimestamp]
		conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Timestamp": {}}}, nil)

		srcType := reflect.TypeFor[TestStruct]()
		targetType := conv.targetType(srcType, "")

		field, ok := targetType.FieldByName("Timestamp")
		require.True(t, ok)
		require.Equal(t, "*int64", field.Type.String())
	})

	t.Run("converts-any-to-string", func(t *testing.T) {
		type TestStruct struct {
			Data any
		}

		rule := RuleRegistry[RuleVariantToString]
		conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{"Data": {}}}, nil)

		srcType := reflect.TypeFor[TestStruct]()
		targetType := conv.targetType(srcType, "")

		field, ok := targetType.FieldByName("Data")
		require.True(t, ok)
		require.Equal(t, "string", field.Type.String())
	})

	t.Run("preserves-non-matching-fields", func(t *testing.T) {
		type TestStruct struct {
			Name  string
			Value int
		}

		rule := RuleRegistry[RuleInt96ToTimestamp]
		conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{}}, nil)

		srcType := reflect.TypeFor[TestStruct]()
		targetType := conv.targetType(srcType, "")

		nameField, ok := targetType.FieldByName("Name")
		require.True(t, ok)
		require.Equal(t, "string", nameField.Type.String())

		valueField, ok := targetType.FieldByName("Value")
		require.True(t, ok)
		require.Equal(t, "int", valueField.Type.String())
	})

	t.Run("caches-type", func(t *testing.T) {
		type UniqueStruct struct {
			UniqueField string
		}

		rule := RuleRegistry[RuleInt96ToTimestamp]
		conv := NewConverter([]*RetypeRule{rule}, []map[string]struct{}{{}}, nil)

		srcType := reflect.TypeFor[UniqueStruct]()
		targetType1 := conv.targetType(srcType, "")
		targetType2 := conv.targetType(srcType, "")

		require.Equal(t, targetType1, targetType2)
	})
}

func TestConvertMap(t *testing.T) {
	t.Run("nil-map-field", func(t *testing.T) {
		type TestStruct struct {
			Name string
			Data map[string]string
		}
		input := &TestStruct{Name: "test", Data: nil}
		conv := NewConverter([]*RetypeRule{RuleRegistry[RuleInt96ToTimestamp]}, []map[string]struct{}{{}}, nil)

		result, err := conv.Convert(input)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("converter-error-on-map-value", func(t *testing.T) {
		type TestStruct struct {
			Data map[string]string
		}
		input := &TestStruct{Data: map[string]string{"key": "not-valid-bson"}}
		conv := NewConverter([]*RetypeRule{RuleRegistry[RuleBsonToString]}, []map[string]struct{}{{"Data\x01Key_value\x01Value": {}}}, nil)

		_, err := conv.Convert(input)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to convert map value")
	})

	t.Run("convert-value-error-on-map-value", func(t *testing.T) {
		type InnerStruct struct {
			Int96 string
		}
		type TestStruct struct {
			Data map[string]InnerStruct
		}
		// "c2hvcnQ=" is base64 for "short" (5 bytes), too short to be a valid INT96 (needs 12)
		input := &TestStruct{Data: map[string]InnerStruct{"key": {Int96: "c2hvcnQ="}}}
		conv := NewConverter([]*RetypeRule{RuleRegistry[RuleInt96ToTimestamp]}, []map[string]struct{}{{"Data\x01Key_value\x01Value\x01Int96": {}}}, nil)

		_, err := conv.Convert(input)
		require.Error(t, err)
	})

	t.Run("map-keys", func(t *testing.T) {
		type TestStruct struct {
			Data map[string]string
		}
		matched := []map[string]struct{}{{"Data\x01Key_value\x01Key": {}}}
		conv := NewConverter([]*RetypeRule{RuleRegistry[RuleFloat16ToFloat32]}, matched, nil)
		testCases := map[string]struct {
			input    map[string]string
			expected map[float32]string
			err      string
		}{
			"converted": {input: map[string]string{"\x00\x3c": "one"}, expected: map[float32]string{1: "one"}},
			// +0 and -0 are distinct FLOAT16 keys but the same FLOAT key
			"collision": {input: map[string]string{"\x00\x00": "a", "\x00\x80": "b"}, err: "map [Data] has keys that both convert to map key"},
			"bad-key":   {input: map[string]string{"x": "v"}, err: "failed to convert map key [x]"},
			"nan-key":   {input: map[string]string{"\x00\x7e": "v"}, err: "map [Data] has a key that converts to NaN"},
		}
		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				result, err := conv.Convert(TestStruct{Data: tc.input})
				if tc.err != "" {
					require.ErrorContains(t, err, tc.err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.expected, reflect.ValueOf(result).Elem().FieldByName("Data").Interface())
			})
		}
	})
}

func TestCmdRejectsNaNMapKey(t *testing.T) {
	type optionalValue struct {
		Data map[string]*string `parquet:"name=data, type=MAP, keytype=FIXED_LEN_BYTE_ARRAY, keylength=2, keylogicaltype=FLOAT16, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8, valuerepetitiontype=OPTIONAL"`
	}
	type requiredValue struct {
		Data map[string]string `parquet:"name=data, type=MAP, keytype=FIXED_LEN_BYTE_ARRAY, keylength=2, keylogicaltype=FLOAT16, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	}
	value := "preserve-me"
	// a NaN key never equals itself, so the writer cannot look its value back up
	testCases := map[string]struct {
		schema any
		row    any
	}{
		"optional-value": {new(optionalValue), optionalValue{Data: map[string]*string{"\x00\x7e": &value}}},
		"required-value": {new(requiredValue), requiredValue{Data: map[string]string{"\x00\x7e": value}}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "source.parquet")
			fw, err := local.NewLocalFileWriter(source)
			require.NoError(t, err)
			pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, tc.schema, writer.WithNP(1))
			require.NoError(t, err)
			require.NoError(t, pw.WriteWithContext(context.Background(), tc.row))
			require.NoError(t, pw.WriteStopWithContext(context.Background()))
			require.NoError(t, fw.Close())

			cmd := Cmd{Float16ToFloat32: true, ReadPageSize: 10, Source: source, URI: filepath.Join(t.TempDir(), "retyped.parquet")}
			require.ErrorContains(t, cmd.Run(context.Background()), "map [Data] has a key that converts to NaN")
		})
	}
}

func TestConverterMatchesFullPath(t *testing.T) {
	validInt96 := types.TimeToINT96(time.Unix(0, 0).UTC())
	type holder struct {
		Ts string
	}
	type row struct {
		A        holder
		B        holder
		List     []holder
		Repeated []string
		Map      map[string]*string
	}
	input := row{
		A:        holder{Ts: validInt96},
		B:        holder{Ts: "123456789012"},
		List:     []holder{{Ts: validInt96}},
		Repeated: []string{validInt96, validInt96},
		Map:      map[string]*string{"k": &validInt96, "nil": nil},
	}
	matched := map[string]struct{}{
		"A\x01Ts":                       {},
		"List\x01List\x01Element\x01Ts": {},
		"Repeated":                      {},
		"Map\x01Key_value\x01Value":     {},
	}
	conv := NewConverter([]*RetypeRule{RuleRegistry[RuleInt96ToTimestamp]}, []map[string]struct{}{matched}, map[string]struct{}{"List": {}})

	result, err := conv.Convert(input)
	require.NoError(t, err)
	got := reflect.ValueOf(result).Elem()
	require.Equal(t, int64(0), got.FieldByName("A").FieldByName("Ts").Interface())
	require.Equal(t, "123456789012", got.FieldByName("B").FieldByName("Ts").Interface())
	require.Equal(t, int64(0), got.FieldByName("List").Index(0).FieldByName("Ts").Interface())
	require.Equal(t, []int64{0, 0}, got.FieldByName("Repeated").Interface())
	gotMap := got.FieldByName("Map")
	require.Equal(t, int64(0), gotMap.MapIndex(reflect.ValueOf("k")).Elem().Interface())
	require.True(t, gotMap.MapIndex(reflect.ValueOf("nil")).IsNil())
	// A and B share a Go type but convert differently, so their target types must differ
	require.NotEqual(t, got.FieldByName("A").Type(), got.FieldByName("B").Type())

	t.Run("nil-slice", func(t *testing.T) {
		result, err := conv.Convert(row{A: holder{Ts: validInt96}})
		require.NoError(t, err)
		require.True(t, reflect.ValueOf(result).Elem().FieldByName("List").IsNil())
	})

	t.Run("nested-errors", func(t *testing.T) {
		type wrapper struct {
			A *holder
			L []holder
		}
		testCases := map[string]struct {
			input   wrapper
			matched string
			err     string
		}{
			"pointer-struct": {wrapper{A: &holder{Ts: "short"}}, "A\x01Ts", "failed to convert field [A.Ts]"},
			"list-element":   {wrapper{L: []holder{{Ts: "short"}}}, "L\x01List\x01Element\x01Ts", "failed to convert list element [0]"},
		}
		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				conv := NewConverter([]*RetypeRule{RuleRegistry[RuleInt96ToTimestamp]}, []map[string]struct{}{{tc.matched: {}}}, map[string]struct{}{"L": {}})
				_, err := conv.Convert(tc.input)
				require.ErrorContains(t, err, tc.err)
			})
		}
	})
}

func TestBsonToJSONString(t *testing.T) {
	t.Run("nan-float-fails-json-marshal", func(t *testing.T) {
		// json.Marshal rejects NaN; BSON supports it, so unmarshal succeeds but marshal fails
		bsonData, err := bson.Marshal(bson.D{{Key: "value", Value: math.NaN()}})
		require.NoError(t, err)

		_, err = bsonToJSONString(string(bsonData))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to marshal to JSON")
	})
}

func TestWriterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	writerChan := make(chan any)

	err := pio.PipelineWriter(ctx, nil, writerChan, "test-target")
	require.ErrorIs(t, err, context.Canceled)
}

func TestReaderContextCancellation(t *testing.T) {
	fileReader, err := pio.NewParquetFileReader(context.Background(), "../../testdata/good.parquet", pio.ReadOption{})
	require.NoError(t, err)
	defer func() { _ = fileReader.PFile.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	converter := NewConverter(nil, nil, nil)
	writerChan := make(chan any) // unbuffered, no receiver

	err = pio.PipelineReader(ctx, fileReader, writerChan, "test", 10, converter.Convert)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCmdRuleMatchesExactColumn(t *testing.T) {
	type int96Holder struct {
		Ts string `parquet:"name=ts, type=INT96"`
	}
	type stringHolder struct {
		Ts string `parquet:"name=ts, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	type row struct {
		A      int96Holder       `parquet:"name=a"`
		B      stringHolder      `parquet:"name=b"`
		Docs   []string          `parquet:"name=docs, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=BSON"`
		Tags   []string          `parquet:"name=tags, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		Attrs  map[string]string `parquet:"name=attrs, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=BSON"`
		Labels map[string]string `parquet:"name=labels, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		Ids    []string          `parquet:"name=ids, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID, repetitiontype=REPEATED"`
		ByID   map[string]string `parquet:"name=by_id, type=MAP, keytype=FIXED_LEN_BYTE_ARRAY, keylength=16, keylogicaltype=UUID, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	}

	doc, err := bson.Marshal(bson.M{"k": "v"})
	require.NoError(t, err)
	ts, err := time.Parse(time.RFC3339, "2022-01-02T03:04:05Z")
	require.NoError(t, err)

	source := filepath.Join(t.TempDir(), "source.parquet")
	fw, err := local.NewLocalFileWriter(source)
	require.NoError(t, err)
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(row), writer.WithNP(1))
	require.NoError(t, err)
	// a 12-byte string looks like INT96 and would be silently reinterpreted on a name collision
	require.NoError(t, pw.WriteWithContext(context.Background(), row{
		A:      int96Holder{Ts: types.TimeToINT96(ts)},
		B:      stringHolder{Ts: "123456789012"},
		Docs:   []string{string(doc)},
		Tags:   []string{"not-bson"},
		Attrs:  map[string]string{"x": string(doc)},
		Labels: map[string]string{"y": "not-bson"},
		Ids:    []string{"0123456789abcdef"},
		ByID:   map[string]string{"0123456789abcdef": "v"},
	}))
	require.NoError(t, pw.WriteStopWithContext(context.Background()))
	require.NoError(t, fw.Close())

	unchanged := []string{`"b":{"ts":"123456789012"}`, `"labels":{"y":"not-bson"}`, `"tags":["not-bson"]`}
	testCases := map[string]struct {
		cmd      Cmd
		expected []string
	}{
		"int96-to-timestamp": {
			cmd:      Cmd{Int96ToTimestamp: true},
			expected: []string{`{"a":{"ts":"2022-01-02T03:04:05.000000000Z"},`},
		},
		"bson-to-string": {
			cmd:      Cmd{BsonToString: true},
			expected: []string{`"attrs":{"x":"{\"k\":\"v\"}"},`, `"docs":["{\"k\":\"v\"}"],`},
		},
		"uuid-to-string": {
			cmd:      Cmd{UuidToString: true},
			expected: []string{`"ids":["30313233-3435-3637-3839-616263646566"]`, `"by_id":{"30313233-3435-3637-3839-616263646566":"v"}`},
		},
		// both rules match ids, so each element is converted before the list is rebuilt
		"uuid-to-string-and-repeated-to-list": {
			cmd:      Cmd{UuidToString: true, RepeatedToList: true},
			expected: []string{`"ids":["30313233-3435-3637-3839-616263646566"]`, `"by_id":{"30313233-3435-3637-3839-616263646566":"v"}`},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tc.cmd.Source = source
			tc.cmd.URI = filepath.Join(t.TempDir(), "retyped.parquet")
			tc.cmd.ReadPageSize = 10
			require.NoError(t, tc.cmd.Run(context.Background()))

			stdout, stderr := testutils.CaptureStdoutStderr(func() {
				cmd := cat.Cmd{ReadPageSize: 10, SampleRatio: 1.0, Format: "jsonl", URI: tc.cmd.URI}
				require.NoError(t, cmd.Run(context.Background()))
			})
			require.Empty(t, stderr)
			for _, expected := range append(tc.expected, unchanged...) {
				require.Contains(t, stdout, expected)
			}
		})
	}
}

func TestConverterAppliesRulesInOrder(t *testing.T) {
	type row struct {
		Repeated []string
	}
	rules := []*RetypeRule{RuleRegistry[RuleInt96ToTimestamp], RuleRegistry[RuleRepeatedToList]}
	matched := []map[string]struct{}{{"Repeated": {}}, {"Repeated": {}}}
	conv := NewConverter(rules, matched, nil)

	testCases := map[string]struct {
		input    row
		expected listWrapper
		err      string
	}{
		"converted-then-wrapped": {
			input:    row{Repeated: []string{types.TimeToINT96(time.Unix(0, 0).UTC())}},
			expected: listWrapper{List: []listElementWrapper{{Element: int64(0)}}},
		},
		"nil-slice":   {input: row{}},
		"bad-element": {input: row{Repeated: []string{"short"}}, err: "failed to convert list element [0]"},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result, err := conv.Convert(tc.input)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, reflect.ValueOf(result).Elem().FieldByName("Repeated").Interface())
		})
	}
}
