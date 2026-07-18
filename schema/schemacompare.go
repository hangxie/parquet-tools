package schema

import "github.com/hangxie/parquet-go/v3/parquet"

// CompareOption controls which fields are included in comparison.
// Zero value compares only the logical schema, ignoring writer directives
// and root node name.
type CompareOption struct {
	CompareEncoding    bool
	CompareCompression bool
	CompareBloomFilter bool
	CompareOmitStats   bool
	CompareRootName    bool
}

// ptrEqual returns true if both pointers are nil or both point to equal values.
func ptrEqual[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// logicalTypeEqual compares two LogicalType pointers using the generated Equals method.
func logicalTypeEqual(a, b *parquet.LogicalType) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equals(b)
}

// logicalSchemaEqual compares the logical schema fields of two SchemaNodes.
// TypeLength is only compared for FIXED_LEN_BYTE_ARRAY types, and Scale/Precision
// are only compared for DECIMAL types, since some writers set these to explicit
// zero pointers for other types while others leave them nil.
func logicalSchemaEqual(a, b *SchemaNode) bool {
	if !ptrEqual(a.Type, b.Type) {
		return false
	}
	if a.Type != nil && *a.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		if !ptrEqual(a.TypeLength, b.TypeLength) {
			return false
		}
	}
	if !ptrEqual(a.RepetitionType, b.RepetitionType) {
		return false
	}
	if a.Name != b.Name {
		return false
	}
	if !ptrEqual(a.ConvertedType, b.ConvertedType) {
		return false
	}
	if a.ConvertedType != nil && *a.ConvertedType == parquet.ConvertedType_DECIMAL {
		if !ptrEqual(a.Scale, b.Scale) || !ptrEqual(a.Precision, b.Precision) {
			return false
		}
	}
	return logicalTypeEqual(a.LogicalType, b.LogicalType)
}

// writerDirectivesEqual compares writer directive fields based on the given option.
func writerDirectivesEqual(a, b *SchemaNode, option CompareOption) bool {
	if option.CompareEncoding && a.Encoding != b.Encoding {
		return false
	}
	if option.CompareCompression && a.CompressionCodec != b.CompressionCodec {
		return false
	}
	if option.CompareBloomFilter && (a.BloomFilter != b.BloomFilter || a.BloomFilterSize != b.BloomFilterSize) {
		return false
	}
	if option.CompareOmitStats && a.OmitStats != b.OmitStats {
		return false
	}
	return true
}

// IsCompatible performs a structural comparison of two SchemaNode trees.
// Logical schema fields (Type, TypeLength, RepetitionType, Name, ConvertedType,
// Scale, Precision, LogicalType, Children) are always compared.
// The root node name is ignored by default; set CompareRootName to compare it.
// Writer directive fields (Encoding, CompressionCodec, BloomFilter, BloomFilterSize,
// OmitStats) are only compared when the corresponding CompareOption flag is set.
// Derived/metadata fields (InNamePath, ExNamePath, FieldID, NumChildren) are always ignored.
func (s *SchemaNode) IsCompatible(other *SchemaNode, option CompareOption) bool {
	if s == nil && other == nil {
		return true
	}
	if s == nil || other == nil {
		return false
	}

	if !option.CompareRootName {
		// Save and temporarily match names so logicalSchemaEqual passes,
		// then enable name comparison for child nodes.
		savedName := other.Name
		other.Name = s.Name
		defer func() { other.Name = savedName }()
		option.CompareRootName = true
	}

	if !logicalSchemaEqual(s, other) {
		return false
	}
	if !writerDirectivesEqual(s, other, option) {
		return false
	}

	if len(s.Children) != len(other.Children) {
		return false
	}
	for i := range s.Children {
		if !s.Children[i].IsCompatible(other.Children[i], option) {
			return false
		}
	}

	return true
}
