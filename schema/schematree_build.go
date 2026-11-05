package schema

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
)

func buildSchemaTree(reader *reader.ParquetReader, option SchemaOption) (*SchemaNode, error) {
	// The tree is built before any metadata read so a malformed schema is reported, not masked by I/O errors.
	schemas := reader.SchemaHandler.SchemaElements
	if len(schemas) == 0 {
		return nil, fmt.Errorf("malformed schema: schema has no elements")
	}
	if schemas[0] == nil {
		return nil, fmt.Errorf("malformed schema: element 0 is nil")
	}
	root := &SchemaNode{
		SchemaElement: *schemas[0],
		Children:      []*SchemaNode{},
		InNamePath:    []string{schemas[0].Name},
		ExNamePath:    strings.Split(reader.SchemaHandler.InPathToExPath[schemas[0].Name], common.ParGoPathDelimiter)[:1],
	}
	stack := []*SchemaNode{root}

	pos := 1
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		if node.GetNumChildren() < 0 {
			return nil, fmt.Errorf("malformed schema: field [%s] has negative num_children %d", node.Name, node.GetNumChildren())
		}
		if option.FailOnInt96 && node.Type != nil && *node.Type == parquet.Type_INT96 {
			return nil, fmt.Errorf("field [%s] has type INT96 which is not supported", node.Name)
		}
		if len(node.Children) < int(node.GetNumChildren()) {
			// Manually assembled readers may bypass footer schema validation.
			if pos >= len(schemas) {
				return nil, fmt.Errorf("malformed schema: field [%s] declares %d children but schema has only %d elements",
					node.Name, node.GetNumChildren(), len(schemas))
			}
			if schemas[pos] == nil {
				return nil, fmt.Errorf("malformed schema: element %d is nil", pos)
			}
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

	if pos != len(schemas) {
		return nil, fmt.Errorf("malformed schema: element %d is outside the root subtree", pos)
	}
	return root, nil
}
