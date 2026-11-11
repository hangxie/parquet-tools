package schema

import (
	"fmt"
	"slices"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	pqschema "github.com/hangxie/parquet-go/v3/schema"
)

func buildSchemaTree(reader *reader.ParquetReader, option SchemaOption) (*SchemaNode, error) {
	// The tree is built before any metadata read so a malformed schema is reported, not masked by I/O errors.
	handler := reader.SchemaHandler
	if handler == nil {
		return nil, fmt.Errorf("malformed schema: schema handler is nil")
	}
	schemas := handler.SchemaElements
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
	}
	var err error
	if root.ExNamePath, err = externalPath(handler, 0, root.InNamePath, nil); err != nil {
		return nil, err
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

			if childNode.ExNamePath, err = externalPath(handler, pos, childNode.InNamePath, node.ExNamePath); err != nil {
				return nil, err
			}

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

// externalPath resolves an element's external path and rejects handler state left stale by schema edits.
func externalPath(handler *pqschema.SchemaHandler, pos int, inPath, parentExPath []string) ([]string, error) {
	inPathStr := strings.Join(inPath, ".")
	exPathKey, found := handler.InPathToExPath[strings.Join(inPath, common.ParGoPathDelimiter)]
	if !found {
		return nil, fmt.Errorf("malformed schema: no external path for element %d [%s]", pos, inPathStr)
	}
	exPath := strings.Split(exPathKey, common.ParGoPathDelimiter)
	exPathStr := strings.Join(exPath, ".")
	if len(exPath) != len(parentExPath)+1 || !slices.Equal(exPath[:len(parentExPath)], parentExPath) {
		return nil, fmt.Errorf("malformed schema: external path [%s] of element %d [%s] does not extend parent path [%s]",
			exPathStr, pos, inPathStr, strings.Join(parentExPath, "."))
	}
	exName := exPath[len(exPath)-1]
	if exName == "" {
		return nil, fmt.Errorf("malformed schema: external path [%s] of element %d [%s] has an empty name", exPathStr, pos, inPathStr)
	}
	// Infos holds the authoritative per-element names when it is aligned with the elements.
	if len(handler.Infos) == len(handler.SchemaElements) && handler.Infos[pos] != nil && handler.Infos[pos].ExName != exName {
		return nil, fmt.Errorf("malformed schema: external name [%s] of element %d [%s] does not match schema info [%s]",
			exName, pos, inPathStr, handler.Infos[pos].ExName)
	}
	return exPath, nil
}
