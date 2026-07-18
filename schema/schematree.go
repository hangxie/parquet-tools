package schema

import (
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
func readFirstDataPageEncoding(pr *reader.ParquetReader, rowGroupIndex, columnIndex int) (parquet.Encoding, error) {
	// Use parquet-go's GetFirstDataPageHeader which correctly handles:
	// - Dictionary pages at DataPageOffset
	// - Proper offset calculation including header sizes
	// - CRC and other page header variations
	headerInfo, err := pr.GetFirstDataPageHeader(rowGroupIndex, columnIndex)
	if err != nil {
		return 0, fmt.Errorf("read first data page header: %w", err)
	}

	return headerInfo.Encoding, nil
}

// buildEncodingMap extracts encoding information from row groups by reading the first data page header.
// For each column, it reads the page header at DataPageOffset to get the actual data page encoding.
// Note: Parquet files should use consistent encodings across row groups for the same column.
// This function reads columns in parallel to speed up remote file access.
func buildEncodingMap(pr *reader.ParquetReader) (map[string]string, error) {
	result := make(map[string]string)

	// Use the first row group to extract encodings
	if len(pr.Footer.RowGroups) == 0 {
		return result, nil
	}

	columns := pr.Footer.RowGroups[0].Columns

	// Use a mutex to protect concurrent writes to the result map
	var mu sync.Mutex

	// Process columns in parallel, use runtime.NumCPU() to match available cores
	g := new(errgroup.Group)
	g.SetLimit(runtime.NumCPU())

	for colIndex, col := range columns {
		g.Go(func() error {
			pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
			if col.GetCryptoMetadata() != nil {
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
			encoding, err := readFirstDataPageEncoding(clonedReader, 0, colIndex)
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

// buildCompressionCodecMap extracts compression codec information from the footer metadata.
// This is a fast operation as it only reads from the already-loaded footer.
func buildCompressionCodecMap(pr *reader.ParquetReader) map[string]string {
	result := make(map[string]string)

	if len(pr.Footer.RowGroups) == 0 {
		return result
	}

	// Use the first row group to extract compression codecs
	columns := pr.Footer.RowGroups[0].Columns
	for _, col := range columns {
		pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
		result[pathKey] = col.MetaData.Codec.String()
	}

	return result
}

// bloomFilterInfo holds bloom filter metadata for a column.
type bloomFilterInfo struct {
	Enabled bool
	Size    int32
}

// buildBloomFilterMap extracts bloom filter information using the correct bitset-only
// size from SchemaHandler.Infos (populated by the library's detectBloomFilters), rather
// than raw metadata which includes Thrift header overhead.
func buildBloomFilterMap(pr *reader.ParquetReader) map[string]bloomFilterInfo {
	result := make(map[string]bloomFilterInfo)

	if len(pr.Footer.RowGroups) == 0 {
		return result
	}

	rootName := pr.SchemaHandler.GetRootInName()
	columns := pr.Footer.RowGroups[0].Columns
	for _, col := range columns {
		if !col.MetaData.IsSetBloomFilterOffset() {
			continue
		}
		pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
		fullPath := common.PathToStr(append([]string{rootName}, col.MetaData.GetPathInSchema()...))
		info := bloomFilterInfo{Enabled: true}
		if idx, ok := pr.SchemaHandler.MapIndex[fullPath]; ok {
			info.Size = pr.SchemaHandler.Infos[idx].BloomFilterSize
		}
		result[pathKey] = info
	}

	return result
}

// BloomFilterSizeMap returns a map from column path (PathInSchema joined) to the correct
// bloom filter bitset size in bytes, as detected by the parquet-go library from the actual
// file header (not from metadata which includes Thrift header overhead).
func BloomFilterSizeMap(pr *reader.ParquetReader) map[string]int32 {
	result := make(map[string]int32)

	if len(pr.Footer.RowGroups) == 0 {
		return result
	}

	rootName := pr.SchemaHandler.GetRootInName()
	columns := pr.Footer.RowGroups[0].Columns
	for _, col := range columns {
		if !col.MetaData.IsSetBloomFilterOffset() {
			continue
		}
		pathKey := strings.Join(col.MetaData.PathInSchema, common.ParGoPathDelimiter)
		fullPath := common.PathToStr(append([]string{rootName}, col.MetaData.GetPathInSchema()...))
		if idx, ok := pr.SchemaHandler.MapIndex[fullPath]; ok {
			result[pathKey] = pr.SchemaHandler.Infos[idx].BloomFilterSize
		}
	}

	return result
}

func NewSchemaTree(reader *reader.ParquetReader, option SchemaOption) (*SchemaNode, error) {
	// Extract encoding information from the parquet file unless SkipPageEncoding is set
	var encodingMap map[string]string
	if !option.SkipPageEncoding {
		var err error
		encodingMap, err = buildEncodingMap(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to build encoding map: %w", err)
		}
	}

	compressionCodecMap := buildCompressionCodecMap(reader)

	// Always extract bloom filter information from footer metadata
	bloomFilterMap := buildBloomFilterMap(reader)

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
