package retype

import (
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"

	pschema "github.com/hangxie/parquet-tools/schema"
)

// nodePath is the node's column path below the root, in the form the converter tracks.
func nodePath(node *pschema.SchemaNode) string {
	if len(node.InNamePath) < 2 {
		return ""
	}
	return strings.Join(node.InNamePath[1:], common.ParGoPathDelimiter)
}

// childPath appends one internal name to a column path.
func childPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + common.ParGoPathDelimiter + name
}

func mapValuePath(path string) string {
	return childPath(childPath(path, "Key_value"), "Value")
}

// displayPath spells a column path the way users write it in messages.
func displayPath(path string) string {
	return strings.ReplaceAll(path, common.ParGoPathDelimiter, ".")
}

// collectListPaths returns the groups parquet-go reads as a Go slice of their List.Element.
func collectListPaths(node *pschema.SchemaNode) map[string]struct{} {
	paths := make(map[string]struct{})
	var walk func(*pschema.SchemaNode)
	walk = func(n *pschema.SchemaNode) {
		if isListGroup(n) {
			paths[nodePath(n)] = struct{}{}
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(node)
	return paths
}

// isListGroup mirrors parquet-go's isListSchema, which decides whether a group becomes a slice.
func isListGroup(n *pschema.SchemaNode) bool {
	annotated := n.GetConvertedType() == parquet.ConvertedType_LIST || (n.LogicalType != nil && n.LogicalType.IsSetLIST())
	if !annotated || len(n.Children) != 1 {
		return false
	}
	wrapper := n.Children[0]
	if wrapper.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED || len(wrapper.Children) != 1 || inName(wrapper) != "List" {
		return false
	}
	element := wrapper.Children[0]
	return element.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED && inName(element) == "Element"
}

func inName(n *pschema.SchemaNode) string {
	if len(n.InNamePath) == 0 {
		return ""
	}
	return n.InNamePath[len(n.InNamePath)-1]
}
