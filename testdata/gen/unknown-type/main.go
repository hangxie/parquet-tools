package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// plainRow uses a regular INT32 field so parquet-go won't reject non-nil values.
// The UNKNOWN logical type is injected into the footer after writing.
type plainRow struct {
	ID         *int32  `parquet:"name=id, type=INT32, repetitiontype=OPTIONAL"`
	UnknownCol *int32  `parquet:"name=unknown_col, type=INT32, repetitiontype=OPTIONAL"`
	Name       *string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}

func main() {
	const outFile = "unknown-type.parquet"

	if err := writeBase(outFile); err != nil {
		fmt.Println("write error:", err)
		return
	}
	if err := patchUnknownLogicalType(outFile, "unknown_col"); err != nil {
		fmt.Println("patch error:", err)
		return
	}
	fmt.Println("Generated", outFile)
}

func writeBase(path string) error {
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return err
	}
	pw, err := writer.NewParquetWriter(
		fw, new(plainRow),
		writer.WithRowGroupSize(128*1024*1024),
		writer.WithPageSize(8*1024),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		return err
	}

	for _, row := range []plainRow{
		{ID: new(int32(1)), UnknownCol: nil, Name: new("alice")},
		{ID: new(int32(2)), UnknownCol: nil, Name: new("bob")},
		{ID: new(int32(3)), UnknownCol: new(int32(30)), Name: new("charlie")},
	} {
		if err := pw.Write(row); err != nil {
			return err
		}
	}
	if err := pw.WriteStop(); err != nil {
		return err
	}
	return fw.Close()
}

// patchUnknownLogicalType reads the parquet file, injects LogicalType{UNKNOWN}
// into the named column's SchemaElement in the Thrift footer, and rewrites the file.
func patchUnknownLogicalType(path, columnName string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < 8 || string(data[len(data)-4:]) != "PAR1" {
		return fmt.Errorf("not a valid parquet file")
	}

	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	footerStart := len(data) - 8 - footerLen
	footerBytes := data[footerStart : footerStart+footerLen]

	// Deserialize footer
	footer := parquet.NewFileMetaData()
	buf := thrift.NewTMemoryBufferLen(len(footerBytes))
	if _, err := buf.Write(footerBytes); err != nil {
		return err
	}
	proto := thrift.NewTCompactProtocolConf(buf, &thrift.TConfiguration{})
	if err := footer.Read(context.Background(), proto); err != nil {
		return err
	}

	// Inject UNKNOWN logical type on the target column
	for _, se := range footer.Schema {
		if se.Name == columnName {
			se.LogicalType = &parquet.LogicalType{UNKNOWN: &parquet.NullType{}}
			break
		}
	}

	// Serialize patched footer
	out := thrift.NewTMemoryBuffer()
	outProto := thrift.NewTCompactProtocolConf(out, &thrift.TConfiguration{})
	if err := footer.Write(context.Background(), outProto); err != nil {
		return err
	}
	newFooter := out.Bytes()

	// Reassemble: original data | new footer | 4-byte length LE | "PAR1"
	var result bytes.Buffer
	result.Write(data[:footerStart])
	result.Write(newFooter)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(newFooter)))
	result.Write(lenBuf)
	result.WriteString("PAR1")

	return os.WriteFile(path, result.Bytes(), 0o644)
}
