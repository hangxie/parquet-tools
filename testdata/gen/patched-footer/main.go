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

// These fixtures mimic files from other writers that parquet-go itself refuses
// to produce, so a valid file is written first and its footer patched after.

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

var shoes = []Shoe{
	{"nike", "air_griffey"},
	{"fila", "grant_hill_2"},
	{"steph_curry", "curry7"},
}

func main() {
	fixtures := []struct {
		name  string
		magic string
		patch func(*parquet.FileMetaData)
	}{
		{"bad-footer-magic.parquet", "XXXX", func(*parquet.FileMetaData) {}},
		{"negative-num-rows.parquet", "PAR1", func(md *parquet.FileMetaData) { md.NumRows = -1 }},
		{"repeated-root.parquet", "PAR1", func(md *parquet.FileMetaData) {
			md.Schema[0].RepetitionType = new(parquet.FieldRepetitionType_REPEATED)
		}},
	}

	for _, f := range fixtures {
		if err := writeBase(f.name); err != nil {
			fmt.Println("write error:", err)
			return
		}
		if err := patchFooter(f.name, f.magic, f.patch); err != nil {
			fmt.Println("patch error:", err)
			return
		}
		fmt.Println("Generated", f.name)
	}
}

func writeBase(path string) error {
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return err
	}
	pw, err := writer.NewParquetWriterWithContext(
		context.Background(),
		fw, new(Shoe),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		_ = fw.Close()
		return err
	}
	for _, s := range shoes {
		if err := pw.WriteWithContext(context.Background(), s); err != nil {
			return err
		}
	}
	if err := pw.WriteStopWithContext(context.Background()); err != nil {
		return err
	}
	return fw.Close()
}

// patchFooter rewrites the Thrift footer through patch and ends the file with magic.
func patchFooter(path, magic string, patch func(*parquet.FileMetaData)) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < 8 || string(data[len(data)-4:]) != "PAR1" {
		return fmt.Errorf("not a valid parquet file")
	}

	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	footerStart := len(data) - 8 - footerLen

	footer := parquet.NewFileMetaData()
	buf := thrift.NewTMemoryBufferLen(footerLen)
	if _, err := buf.Write(data[footerStart : footerStart+footerLen]); err != nil {
		return err
	}
	if err := footer.Read(context.Background(), thrift.NewTCompactProtocolConf(buf, &thrift.TConfiguration{})); err != nil {
		return err
	}

	patch(footer)

	out := thrift.NewTMemoryBuffer()
	if err := footer.Write(context.Background(), thrift.NewTCompactProtocolConf(out, &thrift.TConfiguration{})); err != nil {
		return err
	}

	var result bytes.Buffer
	result.Write(data[:footerStart])
	result.Write(out.Bytes())
	result.Write(binary.LittleEndian.AppendUint32(nil, uint32(out.Len())))
	result.WriteString(magic)
	return os.WriteFile(path, result.Bytes(), 0o644)
}
