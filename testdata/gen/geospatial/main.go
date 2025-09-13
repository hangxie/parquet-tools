package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

type Geospatial struct {
	Geometry  string `parquet:"name=Geometry, type=BYTE_ARRAY, logicaltype=GEOMETRY"`
	Geography string `parquet:"name=Geography, type=BYTE_ARRAY, logicaltype=GEOGRAPHY, omitstats=true"`
}

func main() {
	fw, err := local.NewLocalFileWriter("geospatial.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Geospatial), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	for i := range 10 {
		var geom, geog []byte
		switch i % 7 {
		case 0: // Point
			geom = wkbPoint(float64(i), float64(-i))
			geog = wkbPoint(float64(i)*0.5, float64(i)*1.5)
		case 1: // LineString
			geom = wkbLineString([][]float64{{0, 0}, {float64(i), float64(i)}, {float64(2 * i), float64(-i)}})
			geog = wkbLineString([][]float64{{0.5, 1.5}, {float64(i) + 0.5, float64(i) + 1.5}})
		case 2: // Polygon (single outer ring, closed)
			outer := [][]float64{{0, 0}, {float64(i), 0}, {float64(i), float64(i)}, {0, float64(i)}, {0, 0}}
			geom = wkbPolygon([][][]float64{outer})
			outer2 := [][]float64{{0.5, 0.5}, {float64(i) + 0.5, 0.5}, {float64(i) + 0.5, float64(i) + 0.5}, {0.5, float64(i) + 0.5}, {0.5, 0.5}}
			geog = wkbPolygon([][][]float64{outer2})
		case 3: // MultiPoint
			geom = wkbMultiPoint([][]float64{{float64(i), float64(-i)}, {float64(-i), float64(i)}})
			geog = wkbMultiPoint([][]float64{{float64(i) + 0.5, float64(i) * 1.5}, {float64(i) * 1.5, float64(i) + 0.5}})
		case 4: // MultiLineString
			line1 := [][]float64{{0, 0}, {float64(i), 0}}
			line2 := [][]float64{{0, 0}, {0, float64(i)}}
			geom = wkbMultiLineString([][][]float64{line1, line2})
			line3 := [][]float64{{0.5, 1.5}, {float64(i) + 0.5, float64(i) + 1.5}}
			geog = wkbMultiLineString([][][]float64{line3})
		case 5: // MultiPolygon (two rectangles)
			outerA := [][]float64{{0, 0}, {float64(i), 0}, {float64(i), float64(i)}, {0, float64(i)}, {0, 0}}
			outerB := [][]float64{{float64(i) + 1, float64(i) + 1}, {float64(2*i) + 1, float64(i) + 1}, {float64(2*i) + 1, float64(2*i) + 1}, {float64(i) + 1, float64(2*i) + 1}, {float64(i) + 1, float64(i) + 1}}
			geom = wkbMultiPolygon([][][][]float64{{outerA}, {outerB}})
			outerC := [][]float64{{0.5, 0.5}, {float64(i) + 0.5, 0.5}, {float64(i) + 0.5, float64(i) + 0.5}, {0.5, float64(i) + 0.5}, {0.5, 0.5}}
			outerD := [][]float64{{float64(i) + 0.5, 0.5}, {float64(2*i) + 0.5, 0.5}, {float64(2*i) + 0.5, float64(i) + 0.5}, {float64(i) + 0.5, float64(i) + 0.5}, {float64(i) + 0.5, 0.5}}
			geog = wkbMultiPolygon([][][][]float64{{outerC}, {outerD}})
		default: // GeometryCollection (mix of Point, LineString, Polygon)
			point := wkbPoint(float64(i), float64(-i))
			line := wkbLineString([][]float64{{0, 0}, {float64(i), float64(i)}})
			outer := [][]float64{{0, 0}, {float64(i), 0}, {float64(i), float64(i)}, {0, float64(i)}, {0, 0}}
			poly := wkbPolygon([][][]float64{outer})
			geom = wkbGeometryCollection([][]byte{point, line, poly})

			pointGeog := wkbPoint(float64(i)*0.5, float64(i)*1.5)
			lineGeog := wkbLineString([][]float64{{0.5, 1.5}, {float64(i) + 0.5, float64(i) + 1.5}})
			geog = wkbGeometryCollection([][]byte{pointGeog, lineGeog})
		}

		value := Geospatial{
			Geometry:  string(geom),
			Geography: string(geog),
		}

		if err = pw.Write(value); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()
}

// wkbPoint returns a little-endian WKB for POINT(lon, lat)
func wkbPoint(lon, lat float64) []byte {
	buf := make([]byte, 1+4+8+8)
	// byte order: 1 = little endian
	buf[0] = 1
	// geometry type: 1 = Point (uint32 little endian)
	binary.LittleEndian.PutUint32(buf[1:5], 1)
	// coordinates: lon, lat as float64 little endian
	binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(lon))
	binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(lat))
	return buf
}

func wkbLineString(coords [][]float64) []byte {
	var b bytes.Buffer
	b.WriteByte(1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 2)
	b.Write(t)
	binary.LittleEndian.PutUint32(t, uint32(len(coords)))
	b.Write(t)
	for _, c := range coords {
		x, y := c[0], c[1]
		tmp := make([]byte, 8)
		binary.LittleEndian.PutUint64(tmp, math.Float64bits(x))
		b.Write(tmp)
		binary.LittleEndian.PutUint64(tmp, math.Float64bits(y))
		b.Write(tmp)
	}
	return b.Bytes()
}

func wkbPolygon(rings [][][]float64) []byte {
	var b bytes.Buffer
	b.WriteByte(1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 3)
	b.Write(t)
	binary.LittleEndian.PutUint32(t, uint32(len(rings)))
	b.Write(t)
	for _, ring := range rings {
		binary.LittleEndian.PutUint32(t, uint32(len(ring)))
		b.Write(t)
		for _, c := range ring {
			tmp := make([]byte, 8)
			binary.LittleEndian.PutUint64(tmp, math.Float64bits(c[0]))
			b.Write(tmp)
			binary.LittleEndian.PutUint64(tmp, math.Float64bits(c[1]))
			b.Write(tmp)
		}
	}
	return b.Bytes()
}

func wkbMultiPoint(points [][]float64) []byte {
	var b bytes.Buffer
	b.WriteByte(1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 4)
	b.Write(t)
	binary.LittleEndian.PutUint32(t, uint32(len(points)))
	b.Write(t)
	for _, p := range points {
		b.Write(wkbPoint(p[0], p[1]))
	}
	return b.Bytes()
}

func wkbMultiLineString(lines [][][]float64) []byte {
	var b bytes.Buffer
	b.WriteByte(1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 5)
	b.Write(t)
	binary.LittleEndian.PutUint32(t, uint32(len(lines)))
	b.Write(t)
	for _, l := range lines {
		b.Write(wkbLineString(l))
	}
	return b.Bytes()
}

func wkbMultiPolygon(polys [][][][]float64) []byte {
	var b bytes.Buffer
	b.WriteByte(1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 6)
	b.Write(t)
	binary.LittleEndian.PutUint32(t, uint32(len(polys)))
	b.Write(t)
	for _, poly := range polys {
		b.Write(wkbPolygon(poly))
	}
	return b.Bytes()
}

func wkbGeometryCollection(geometries [][]byte) []byte {
	var b bytes.Buffer
	b.WriteByte(1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 7)
	b.Write(t)
	binary.LittleEndian.PutUint32(t, uint32(len(geometries)))
	b.Write(t)
	for _, geom := range geometries {
		b.Write(geom)
	}
	return b.Bytes()
}
