package cat

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func rawValue(value reflect.Value, path string, handler *schema.SchemaHandler) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		return rawValue(value.Elem(), path, handler)
	}
	index, ok := handler.MapIndex[path]
	if !ok || index < 0 || int(index) >= len(handler.SchemaElements) {
		return nil, fmt.Errorf("raw value schema path %q is missing", strings.ReplaceAll(path, common.ParGoPathDelimiter, "."))
	}
	element := handler.SchemaElements[index]
	if element.LogicalType != nil && element.LogicalType.IsSetVARIANT() {
		if variant, ok := value.Interface().(types.Variant); ok {
			return types.ConvertVariantValue(variant)
		}
		return value.Interface(), nil
	}
	if element.Type != nil && (value.Kind() != reflect.Slice || (value.Type().Elem().Kind() == reflect.Uint8 && (*element.Type == parquet.Type_BYTE_ARRAY || *element.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY))) {
		converted, err := types.ConvertValue(value.Interface(), element, types.WithValueMode(types.ValueModeRaw))
		if err != nil {
			return nil, fmt.Errorf("raw value at %q: %w", path, err)
		}
		return converted, nil
	}
	switch value.Kind() {
	case reflect.Slice:
		elementPath := path
		if element.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED {
			elementPath += common.ParGoPathDelimiter + "List" + common.ParGoPathDelimiter + "Element"
		}
		result := make([]any, value.Len())
		for i := range value.Len() {
			converted, err := rawValue(value.Index(i), elementPath, handler)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case reflect.Map:
		return rawMap(value, path, handler)
	case reflect.Struct:
		return rawStruct(value, path, handler)
	default:
		return nil, fmt.Errorf("raw group %q has unexpected value %T", path, value.Interface())
	}
}

func rawMap(value reflect.Value, path string, handler *schema.SchemaHandler) (any, error) {
	result := make(map[string]any, value.Len())
	entryPath := path + common.ParGoPathDelimiter + "Key_value" + common.ParGoPathDelimiter
	iter := value.MapRange()
	for iter.Next() {
		key, err := rawValue(iter.Key(), entryPath+"Key", handler)
		if err != nil {
			return nil, err
		}
		converted, err := rawValue(iter.Value(), entryPath+"Value", handler)
		if err != nil {
			return nil, err
		}
		result[fmt.Sprint(key)] = converted
	}
	return result, nil
}

func rawStruct(value reflect.Value, path string, handler *schema.SchemaHandler) (any, error) {
	if value.NumField() == 1 && value.Type().Field(0).IsExported() &&
		strings.EqualFold(value.Type().Field(0).Name, "array") && value.Field(0).Kind() == reflect.Slice &&
		strings.Split(value.Type().Field(0).Tag.Get("json"), ",")[0] != "-" {
		return rawValue(value.Field(0), path+common.ParGoPathDelimiter+value.Type().Field(0).Name, handler)
	}
	result := make(map[string]any, value.NumField())
	for i := range value.NumField() {
		field := value.Type().Field(i)
		if !field.IsExported() {
			continue
		}
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		converted, err := rawValue(value.Field(i), path+common.ParGoPathDelimiter+field.Name, handler)
		if err != nil {
			return nil, err
		}
		result[name] = converted
	}
	return result, nil
}
