package main

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// A plaintext footer names every bloom filter without a key, while sizing one reads
// the filter header from an encrypted chunk. Metadata-only commands must still work.
// Filtered columns cover all three ways a chunk can be keyed: a column key, the
// footer key, and no encryption at all.
type EncryptedBloomFilterData struct {
	ID     int64   `parquet:"name=ID, type=INT64, bloomfilter=true"`
	Name   string  `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true, bloomfiltersize=4096"`
	Age    int32   `parquet:"name=Age, type=INT32"`
	Footer float64 `parquet:"name=Footer, type=DOUBLE, bloomfilter=true, bloomfiltersize=2048"`
}

// Same keys the golden script passes as ENC_FOOTER_KEY and ENC_DOUBLE_KEY.
var (
	footerKey = []byte("0123456789012345")
	columnKey = []byte("1234567890123450")
)

func main() {
	fw, err := local.NewLocalFileWriter("encrypted-bloom-filter.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(EncryptedBloomFilterData),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		writer.WithFooterKey(footerKey),
		writer.WithPlaintextFooter(true),
		// Name stays plaintext, so its filter is sizable with no key at all.
		writer.WithColumnEncrypted("ID", writer.ColumnKey(columnKey)),
		// Keyed by the footer rather than by itself, the other way a chunk can be
		// unreadable without turning that into a failed command.
		writer.WithColumnEncrypted("Footer", writer.ColumnFooterKey()),
	)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}
	for i := range 10 {
		value := EncryptedBloomFilterData{
			ID:     int64(i),
			Name:   fmt.Sprintf("name-%d", i),
			Age:    int32(20 + i),
			Footer: float64(i) * 1.5,
		}
		if err = pw.WriteWithContext(context.Background(), value); err != nil {
			fmt.Println("Write error", err)
			return
		}
	}
	if err = pw.WriteStopWithContext(context.Background()); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}
