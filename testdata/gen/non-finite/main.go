package main

import (
	"context"
	"log"
	"math"

	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Every floating point shape a JSON encoder has to walk: a bare field, an
// optional one, a half-precision one, a list element, a map value, a field
// inside a nested group, and a legacy REPEATED field for retype to rewrite.
//
// Rep holds finite values only: as of parquet-go v3.8.1 its JSON conversion
// looks up a LIST/Element schema path for every slice, which a legacy REPEATED
// column does not have, so its values never reach the logical type conversion.
//
// The writer keeps infinities in column statistics but drops NaN, so this file
// exercises non-finite statistics while nan.parquet, written before that rule,
// still covers reading NaN out of min/max.
type Data struct {
	Dbl    float64            `parquet:"name=dbl, type=DOUBLE"`
	Flt    float32            `parquet:"name=flt, type=FLOAT"`
	OptDbl *float64           `parquet:"name=opt_dbl, type=DOUBLE, repetitiontype=OPTIONAL"`
	Half   string             `parquet:"name=half, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16"`
	List   []float64          `parquet:"name=list, type=LIST, valuetype=DOUBLE"`
	Scores map[string]float64 `parquet:"name=scores, type=MAP, keytype=BYTE_ARRAY, keylogicaltype=STRING, valuetype=DOUBLE"`
	Rep    []float64          `parquet:"name=rep, type=DOUBLE, repetitiontype=REPEATED"`
	Nested struct {
		Inner float32 `parquet:"name=inner, type=FLOAT"`
	} `parquet:"name=nested"`
}

// IEEE 754 half-precision bit patterns, little endian.
const (
	half16NaN    = "\x00\x7e"
	half16PosInf = "\x00\x7c"
	half16NegInf = "\x00\xfc"
	half16Finite = "\x00\x3e" // 1.5
)

func main() {
	posInf := math.Inf(1)
	negInf := math.Inf(-1)
	finite := 3.25

	rows := []Data{
		{
			Dbl: math.NaN(), Flt: float32(math.NaN()), OptDbl: nil, Half: half16NaN,
			List: []float64{math.NaN(), 1.5}, Scores: map[string]float64{"a": math.NaN()},
			Rep: []float64{1.5},
		},
		{
			Dbl: posInf, Flt: float32(posInf), OptDbl: &posInf, Half: half16PosInf,
			List: []float64{posInf}, Scores: map[string]float64{"b": posInf},
			Rep: []float64{2.5, 3.5},
		},
		{
			Dbl: negInf, Flt: float32(negInf), OptDbl: &negInf, Half: half16NegInf,
			List: []float64{negInf}, Scores: map[string]float64{"c": negInf},
			Rep: []float64{4.5},
		},
		{
			Dbl: finite, Flt: 1.5, OptDbl: &finite, Half: half16Finite,
			List: []float64{2.5}, Scores: map[string]float64{"d": 4.5},
			Rep: []float64{9.5},
		},
	}
	rows[0].Nested.Inner = float32(math.NaN())
	rows[1].Nested.Inner = float32(posInf)
	rows[2].Nested.Inner = float32(negInf)
	rows[3].Nested.Inner = 9.5

	file, err := local.NewLocalFileWriter("non-finite.parquet")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}

	pw, err := writer.NewParquetWriterWithContext(context.Background(), file, new(Data), writer.WithNP(1))
	if err != nil {
		log.Fatalf("Failed to create parquet writer: %v", err)
	}

	for _, row := range rows {
		if err := pw.WriteWithContext(context.Background(), row); err != nil {
			log.Fatalf("Failed to write data: %v", err)
		}
	}

	if err := pw.WriteStopWithContext(context.Background()); err != nil {
		log.Fatalf("Failed to close parquet writer: %v", err)
	}

	if err := file.Close(); err != nil {
		log.Fatalf("Failed to close file: %v", err)
	}
}
