package retype

import (
	"fmt"
	"reflect"
)

// FieldConverter pairs a rule with its matched fields.
type FieldConverter struct {
	Rule   *RetypeRule
	Fields map[string]struct{}
}

// Converter handles data conversion for multiple rules.
type Converter struct {
	converters      []*FieldConverter
	typeCache       map[reflect.Type]reflect.Type
	needsTypeChange bool
}

// NewConverter creates a converter for the given rules and their matched fields.
func NewConverter(rules []*RetypeRule, matchedFields []map[string]struct{}) *Converter {
	converters := make([]*FieldConverter, 0, len(rules))
	needsTypeChange := false

	for i, rule := range rules {
		if rule.ConvertData == nil {
			continue
		}
		converters = append(converters, &FieldConverter{
			Rule:   rule,
			Fields: matchedFields[i],
		})
		if rule.TargetType != nil {
			needsTypeChange = true
		}
	}

	return &Converter{
		converters:      converters,
		typeCache:       make(map[reflect.Type]reflect.Type),
		needsTypeChange: needsTypeChange,
	}
}

// Convert transforms a row according to all active rules.
func (c *Converter) Convert(row any) (any, error) {
	if len(c.converters) == 0 {
		return row, nil
	}
	return c.convertValue(reflect.ValueOf(row))
}

// convertValue recursively converts a reflect.Value.
func (c *Converter) convertValue(srcVal reflect.Value) (any, error) {
	if srcVal.Kind() == reflect.Pointer {
		if srcVal.IsNil() {
			return nil, nil
		}
		result, err := c.convertValue(srcVal.Elem())
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, nil
		}
		resultVal := reflect.ValueOf(result)
		if resultVal.Kind() == reflect.Pointer {
			return result, nil
		}
		ptr := reflect.New(resultVal.Type())
		ptr.Elem().Set(resultVal)
		return ptr.Interface(), nil
	}

	switch srcVal.Kind() {
	case reflect.Struct:
		return c.convertStruct(srcVal)
	case reflect.Slice:
		return c.convertSlice(srcVal)
	case reflect.Map:
		return c.convertMap(srcVal)
	default:
		return srcVal.Interface(), nil
	}
}

// convertStruct converts a struct value.
func (c *Converter) convertStruct(srcVal reflect.Value) (any, error) {
	srcType := srcVal.Type()
	targetType := c.getOrCreateTargetType(srcType)
	targetVal := reflect.New(targetType).Elem()

	for i := range srcType.NumField() {
		srcField := srcType.Field(i)
		srcFieldVal := srcVal.Field(i)
		targetFieldVal := targetVal.Field(i)

		converter := c.findConverterForField(srcField.Name)
		if converter != nil {
			converted, err := c.convertField(srcFieldVal, converter.Rule, srcField.Name)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				targetFieldVal.Set(convertedVal)
			}
		} else {
			// Recursively convert nested types
			converted, err := c.convertValue(srcFieldVal)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				if convertedVal.Kind() == reflect.Pointer && targetFieldVal.Kind() != reflect.Pointer {
					convertedVal = convertedVal.Elem()
				}
				targetFieldVal.Set(convertedVal)
			}
		}
	}

	return targetVal.Addr().Interface(), nil
}

// convertField applies a rule's conversion to a single field.
func (c *Converter) convertField(srcVal reflect.Value, rule *RetypeRule, fieldName string) (any, error) {
	if srcVal.Kind() == reflect.Pointer {
		if srcVal.IsNil() {
			return c.nilPointerForRule(rule), nil
		}
		// Dereference pointer for conversion, but note that the rule might expect the value itself
		// For consistency with how we handle primitives, we pass the element value.
		// However, since we now accept 'any', we can just pass srcVal.Elem().Interface()
		result, err := rule.ConvertData(srcVal.Elem().Interface())
		if err != nil {
			return nil, fmt.Errorf("failed to convert field [%s]: %w", fieldName, err)
		}
		// Wrap in pointer
		resultVal := reflect.ValueOf(result)
		ptr := reflect.New(resultVal.Type())
		ptr.Elem().Set(resultVal)
		return ptr.Interface(), nil
	}

	// Direct value conversion (string, or any other type for VARIANT)
	result, err := rule.ConvertData(srcVal.Interface())
	if err != nil {
		return nil, fmt.Errorf("failed to convert field [%s]: %w", fieldName, err)
	}
	return result, nil
}

// nilPointerForRule returns a nil pointer of the appropriate type.
func (c *Converter) nilPointerForRule(rule *RetypeRule) any {
	if rule.TargetType != nil {
		return reflect.Zero(reflect.PointerTo(rule.TargetType)).Interface()
	}
	return (*string)(nil)
}

// convertSlice converts each element of a slice.
func (c *Converter) convertSlice(srcVal reflect.Value) (any, error) {
	if srcVal.IsNil() {
		return nil, nil
	}

	elemType := c.getOrCreateTargetTypeForField(srcVal.Type().Elem())
	targetSlice := reflect.MakeSlice(reflect.SliceOf(elemType), srcVal.Len(), srcVal.Len())

	// Check for Element/element fields (Parquet LIST elements)
	converter := c.findConverterForField("Element")
	if converter == nil {
		converter = c.findConverterForField("element")
	}

	for i := range srcVal.Len() {
		elem := srcVal.Index(i)

		if converter != nil && (converter.Rule.InputKind == reflect.Invalid || elem.Kind() == converter.Rule.InputKind) {
			// Rule application
			valToConvert := elem.Interface()
			result, err := converter.Rule.ConvertData(valToConvert)
			if err != nil {
				return nil, fmt.Errorf("failed to convert list element [%d]: %w", i, err)
			}
			targetSlice.Index(i).Set(reflect.ValueOf(result))
		} else {
			converted, err := c.convertValue(elem)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				if convertedVal.Kind() == reflect.Pointer && elemType.Kind() != reflect.Pointer {
					convertedVal = convertedVal.Elem()
				}
				targetSlice.Index(i).Set(convertedVal)
			}
		}
	}

	return targetSlice.Interface(), nil
}

// convertMap converts each value of a map.
func (c *Converter) convertMap(srcVal reflect.Value) (any, error) {
	if srcVal.IsNil() {
		return nil, nil
	}

	keyType := srcVal.Type().Key()
	valueType := c.getOrCreateTargetTypeForField(srcVal.Type().Elem())
	targetMap := reflect.MakeMap(reflect.MapOf(keyType, valueType))

	// Check for Value/value fields (Parquet MAP values)
	converter := c.findConverterForField("Value")
	if converter == nil {
		converter = c.findConverterForField("value")
	}

	iter := srcVal.MapRange()
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()

		if converter != nil && (converter.Rule.InputKind == reflect.Invalid || val.Kind() == converter.Rule.InputKind) {
			// Rule application
			valToConvert := val.Interface()
			result, err := converter.Rule.ConvertData(valToConvert)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map value [%v]: %w", key.Interface(), err)
			}
			targetMap.SetMapIndex(key, reflect.ValueOf(result))
		} else {
			converted, err := c.convertValue(val)
			if err != nil {
				return nil, err
			}
			if converted != nil {
				convertedVal := reflect.ValueOf(converted)
				if convertedVal.Kind() == reflect.Pointer && valueType.Kind() != reflect.Pointer {
					convertedVal = convertedVal.Elem()
				}
				targetMap.SetMapIndex(key, convertedVal)
			}
		}
	}

	return targetMap.Interface(), nil
}

// getOrCreateTargetType creates a target struct type with converted field types.
func (c *Converter) getOrCreateTargetType(srcType reflect.Type) reflect.Type {
	if cached, ok := c.typeCache[srcType]; ok {
		return cached
	}

	fields := make([]reflect.StructField, srcType.NumField())
	for i := range srcType.NumField() {
		srcField := srcType.Field(i)
		fields[i] = srcField

		converter := c.findConverterForField(srcField.Name)
		if converter != nil && converter.Rule.TargetType != nil {
			targetType := converter.Rule.TargetType
			if srcField.Type.Kind() == reflect.Pointer {
				fields[i].Type = reflect.PointerTo(targetType)
			} else {
				fields[i].Type = targetType
			}
			// Clear tag to avoid conflicting repetition information (e.g. repetition=repeated on a struct field)
			fields[i].Tag = ""
		} else {
			fields[i].Type = c.getOrCreateTargetTypeForField(srcField.Type)
		}
	}

	targetType := reflect.StructOf(fields)
	c.typeCache[srcType] = targetType
	return targetType
}

// getOrCreateTargetTypeForField creates target types for nested fields.
func (c *Converter) getOrCreateTargetTypeForField(srcType reflect.Type) reflect.Type {
	switch srcType.Kind() {
	case reflect.Struct:
		return c.getOrCreateTargetType(srcType)
	case reflect.Slice:
		elemType := c.getOrCreateTargetTypeForField(srcType.Elem())
		return reflect.SliceOf(elemType)
	case reflect.Map:
		keyType := srcType.Key()
		valueType := c.getOrCreateTargetTypeForField(srcType.Elem())
		return reflect.MapOf(keyType, valueType)
	case reflect.Pointer:
		elemType := c.getOrCreateTargetTypeForField(srcType.Elem())
		return reflect.PointerTo(elemType)
	default:
		return srcType
	}
}

// findConverterForField returns the converter that handles the given field name.
func (c *Converter) findConverterForField(fieldName string) *FieldConverter {
	for _, conv := range c.converters {
		if _, ok := conv.Fields[fieldName]; ok {
			return conv
		}
	}
	return nil
}
