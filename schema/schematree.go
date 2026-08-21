package schema

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"golang.org/x/sync/errgroup"
)

// readFirstDataPageEncoding reads the first data page header to get its encoding.
// Uses the parquet-go library's GetFirstDataPageHeader which efficiently reads only
// the first data page, skipping any dictionary pages.
func readFirstDataPageEncoding(ctx context.Context, pr *reader.ParquetReader, rowGroupIndex, columnIndex int) (parquet.Encoding, error) {
	headerInfo, err := pr.GetFirstDataPageHeaderWithContext(ctx, rowGroupIndex, columnIndex)
	if err != nil {
		return 0, fmt.Errorf("read first data page header: %w", err)
	}

	return headerInfo.Encoding, nil
}

// firstNonEmptyChunk returns the first row group holding values for a column.
func firstNonEmptyChunk(rowGroups []*parquet.RowGroup, colIndex int) (int, *parquet.ColumnChunk, bool) {
	// An empty chunk has no data page, and its DataPageOffset of 0 would aim a
	// read at the file magic.
	for rgIndex, rowGroup := range rowGroups {
		if colIndex >= len(rowGroup.Columns) {
			continue
		}
		chunk := rowGroup.Columns[colIndex]
		if chunk == nil || chunk.MetaData == nil || chunk.MetaData.GetNumValues() == 0 {
			continue
		}
		return rgIndex, chunk, true
	}
	return 0, nil, false
}

// representativeChunk returns the chunk that speaks for a column, or fallback when
// every chunk is empty. A schema leaf holds one value, so per-chunk metadata is sampled.
func representativeChunk(rowGroups []*parquet.RowGroup, colIndex int, fallback *parquet.ColumnChunk) *parquet.ColumnChunk {
	if _, chunk, ok := firstNonEmptyChunk(rowGroups, colIndex); ok {
		return chunk
	}
	return fallback
}

// firstChunkWithBloomFilter returns the first row group carrying a bloom filter offset.
func firstChunkWithBloomFilter(rowGroups []*parquet.RowGroup, colIndex int) (int, *parquet.ColumnChunk, bool) {
	for rgIndex, rowGroup := range rowGroups {
		if colIndex >= len(rowGroup.Columns) {
			continue
		}
		chunk := rowGroup.Columns[colIndex]
		if chunk == nil || chunk.MetaData == nil || !chunk.MetaData.IsSetBloomFilterOffset() {
			continue
		}
		return rgIndex, chunk, true
	}
	return 0, nil, false
}

// buildEncodingMap maps each column path to the encoding of its first data page.
// Encodings are consistent across row groups, so one chunk speaks for the column.
func buildEncodingMap(ctx context.Context, pr *reader.ParquetReader) (map[string]string, error) {
	result := make(map[string]string)

	if len(pr.Footer.RowGroups) == 0 {
		return result, nil
	}

	// Row group 0 supplies the column list only.
	columns := pr.Footer.RowGroups[0].Columns

	// Use a mutex to protect concurrent writes to the result map
	var mu sync.Mutex

	// Process columns in parallel, use runtime.NumCPU() to match available cores
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())

	for colIndex, col := range columns {
		g.Go(func() error {
			// Without metadata there is no column path, and a panic here would
			// take down the process from inside the goroutine.
			if col == nil || col.MetaData == nil {
				return nil
			}
			pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)

			rgIndex, chunk, ok := firstNonEmptyChunk(pr.Footer.RowGroups, colIndex)
			if !ok || chunk.GetCryptoMetadata() != nil {
				return nil
			}

			// Clone the reader to get a dedicated file handle for concurrent access
			// This is necessary because io.ReadSeeker operations (Seek/Read) are not thread-safe
			clonedFile, err := pr.PFile.Clone()
			if err != nil {
				return fmt.Errorf("failed to clone file for column [%s]: %w", pathKey, err)
			}
			defer func() {
				_ = clonedFile.Close()
			}()

			// Create a temporary reader with the cloned file
			clonedReader := &reader.ParquetReader{
				PFile:         clonedFile,
				Footer:        pr.Footer,
				SchemaHandler: pr.SchemaHandler,
			}

			// Read just the first data page header to get encoding
			encoding, err := readFirstDataPageEncoding(gctx, clonedReader, rgIndex, colIndex)
			if err != nil {
				return fmt.Errorf("failed to read encoding for column [%s]: %w", pathKey, err)
			}

			mu.Lock()
			result[pathKey] = encoding.String()
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

// buildCompressionCodecMap maps each column path to the codec of its representative
// chunk, read from the already-loaded footer. Files write one codec per column.
func buildCompressionCodecMap(pr *reader.ParquetReader) map[string]string {
	result := make(map[string]string)

	if len(pr.Footer.RowGroups) == 0 {
		return result
	}

	for colIndex, col := range pr.Footer.RowGroups[0].Columns {
		if col == nil || col.MetaData == nil {
			continue
		}
		pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
		// An empty chunk records a codec no data was written with.
		chunk := representativeChunk(pr.Footer.RowGroups, colIndex, col)
		result[pathKey] = chunk.MetaData.Codec.String()
	}

	return result
}

// bloomFilterInfo holds bloom filter metadata for a column.
type bloomFilterInfo struct {
	Enabled bool
	Size    int32
}

// buildBloomFilterMap reports which columns carry a bloom filter, sized in bitset bytes.
// The offset is per chunk, so a column filtered in any row group counts as filtered.
func buildBloomFilterMap(ctx context.Context, pr *reader.ParquetReader) (map[string]bloomFilterInfo, error) {
	result := make(map[string]bloomFilterInfo)

	if len(pr.Footer.RowGroups) == 0 {
		return result, nil
	}

	for colIndex, col := range pr.Footer.RowGroups[0].Columns {
		if col == nil || col.MetaData == nil {
			continue
		}
		rgIndex, _, ok := firstChunkWithBloomFilter(pr.Footer.RowGroups, colIndex)
		if !ok {
			continue
		}
		pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
		info := bloomFilterInfo{Enabled: true}
		// Sizing reads the filter header, which an encrypted column yields only to a
		// key the caller may not hold. Presence came from the footer, so leave it unsized.
		size, err := pr.BloomFilterSize(ctx, pathKey, rgIndex)
		switch {
		case err == nil:
			info.Size = size
		case !errors.Is(err, reader.ErrColumnKeyRequired):
			return nil, fmt.Errorf("failed to read bloom filter size for column [%s]: %w", pathKey, err)
		}
		result[pathKey] = info
	}

	return result, nil
}

func NewSchemaTree(ctx context.Context, reader *reader.ParquetReader, option SchemaOption) (*SchemaNode, error) {
	// Extract encoding information from the parquet file unless SkipPageEncoding is set
	var encodingMap map[string]string
	if !option.SkipPageEncoding {
		var err error
		encodingMap, err = buildEncodingMap(ctx, reader)
		if err != nil {
			return nil, fmt.Errorf("failed to build encoding map: %w", err)
		}
	}

	compressionCodecMap := buildCompressionCodecMap(reader)

	// Sizing a bloom filter reads its header, so a caller that does not report
	// bloom filters can opt out of that read.
	var bloomFilterMap map[string]bloomFilterInfo
	if !option.SkipBloomFilter {
		var err error
		bloomFilterMap, err = buildBloomFilterMap(ctx, reader)
		if err != nil {
			return nil, err
		}
	}

	schemas := reader.SchemaHandler.SchemaElements
	root := &SchemaNode{
		SchemaElement: *schemas[0],
		Children:      []*SchemaNode{},
		InNamePath:    []string{schemas[0].Name},
		ExNamePath:    strings.Split(reader.SchemaHandler.InPathToExPath[schemas[0].Name], common.ParGoPathDelimiter)[:1],
	}
	stack := []*SchemaNode{root}

	for pos := 1; len(stack) > 0; {
		node := stack[len(stack)-1]
		if option.FailOnInt96 && node.Type != nil && *node.Type == parquet.Type_INT96 {
			return nil, fmt.Errorf("field [%s] has type INT96 which is not supported", node.Name)
		}
		if len(node.Children) < int(node.GetNumChildren()) {
			childNode := &SchemaNode{
				SchemaElement: *schemas[pos],
				Children:      []*SchemaNode{},
			}

			// append() does not always return new slice, so we need to copy the old slice
			childNode.InNamePath = make([]string, len(node.InNamePath)+1)
			copy(childNode.InNamePath, node.InNamePath)
			childNode.InNamePath[len(node.InNamePath)] = schemas[pos].Name

			inPathKey := strings.Join(childNode.InNamePath, common.ParGoPathDelimiter)
			childNode.ExNamePath = strings.Split(reader.SchemaHandler.InPathToExPath[inPathKey], common.ParGoPathDelimiter)

			node.Children = append(node.Children, childNode)
			stack = append(stack, childNode)
			pos++
		} else {
			stack = stack[:len(stack)-1]
			if len(node.Children) == 0 {
				node.Children = nil
			}
		}
	}

	populateLeafMetadata(root, encodingMap, compressionCodecMap, bloomFilterMap)
	markUndefinedSortOrder(root)
	return root, nil
}

func populateLeafMetadata(root *SchemaNode, encodingMap, compressionCodecMap map[string]string, bloomFilterMap map[string]bloomFilterInfo) {
	queue := []*SchemaNode{root}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)
		node.Name = node.ExNamePath[len(node.ExNamePath)-1]

		if node.Type != nil {
			pathKey := strings.Join(node.InNamePath[1:], common.ParGoPathDelimiter)
			if encodingMap != nil {
				if encoding, found := encodingMap[pathKey]; found {
					node.Encoding = encoding
				}
			}
			if compressionCodecMap != nil {
				if codec, found := compressionCodecMap[pathKey]; found {
					node.CompressionCodec = codec
				}
			}
			if info, found := bloomFilterMap[pathKey]; found && info.Enabled {
				node.BloomFilter = "true"
				if info.Size > 0 {
					node.BloomFilterSize = fmt.Sprint(info.Size)
				}
			}
		}
	}
}

// markUndefinedSortOrder recursively marks nodes whose sort order is
// undefined per the Parquet spec, so that DecodeStatistics skips min/max.
//   - GEOMETRY, GEOGRAPHY: marked on the node itself (leaf with logical type)
//   - INTERVAL: marked on the node itself (leaf with converted type)
//   - VARIANT: a STRUCT whose logical type is on the parent, so all descendants are marked
func markUndefinedSortOrder(node *SchemaNode) {
	if node.LogicalType != nil {
		if node.LogicalType.IsSetGEOMETRY() || node.LogicalType.IsSetGEOGRAPHY() || node.LogicalType.IsSetUNKNOWN() {
			node.UndefinedSortOrder = true
			return
		}
		if node.LogicalType.IsSetVARIANT() {
			markAllDescendants(node)
			return
		}
	}
	if node.ConvertedType != nil && *node.ConvertedType == parquet.ConvertedType_INTERVAL {
		node.UndefinedSortOrder = true
		return
	}
	for _, child := range node.Children {
		markUndefinedSortOrder(child)
	}
}

func markAllDescendants(node *SchemaNode) {
	for _, child := range node.Children {
		child.UndefinedSortOrder = true
		markAllDescendants(child)
	}
}
