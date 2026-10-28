package retype

import (
	"fmt"
	"reflect"
)

// FieldConverter pairs a rule with the column paths it matched.
type FieldConverter struct {
	Rule   *RetypeRule
	Fields map[string]struct{}
}

// typeKey identifies a target type; identical Go types can sit at columns that convert differently.
type typeKey struct {
	srcType reflect.Type
	path    string
}

// Converter handles data conversion for multiple rules.
type Converter struct {
	converters []*FieldConverter
	listPaths  map[string]struct{}
	typeCache  map[typeKey]reflect.Type
}

// NewConverter creates a converter; listPaths are the source LIST groups rows carry as slices.
func NewConverter(rules []*RetypeRule, matchedFields []map[string]struct{}, listPaths map[string]struct{}) *Converter {
	converters := make([]*FieldConverter, 0, len(rules))
	for i, rule := range rules {
		if rule.ConvertData == nil {
			continue
		}
		converters = append(converters, &FieldConverter{
			Rule:   rule,
			Fields: matchedFields[i],
		})
	}

	return &Converter{
		converters: converters,
		listPaths:  listPaths,
		typeCache:  make(map[typeKey]reflect.Type),
	}
}

// Convert transforms a row according to all active rules.
func (c *Converter) Convert(row any) (any, error) {
	if len(c.converters) == 0 {
		return row, nil
	}
	srcVal := reflect.ValueOf(row)
	if !srcVal.IsValid() || (srcVal.Kind() == reflect.Pointer && srcVal.IsNil()) {
		return nil, nil
	}
	if srcVal.Kind() == reflect.Pointer {
		srcVal = srcVal.Elem()
	}

	result, err := c.convertValue(srcVal, "")
	if err != nil {
		return nil, err
	}
	if result.Kind() != reflect.Struct {
		return result.Interface(), nil
	}
	ptr := reflect.New(result.Type())
	ptr.Elem().Set(result)
	return ptr.Interface(), nil
}

// convertValue converts the value found at the column path into its target type.
func (c *Converter) convertValue(srcVal reflect.Value, path string) (reflect.Value, error) {
	if rules := c.rulesAt(path); len(rules) != 0 {
		var err error
		for _, rule := range rules {
			if srcVal, err = convertWithRule(srcVal, rule, path); err != nil {
				return reflect.Value{}, err
			}
		}
		return srcVal, nil
	}

	targetType := c.targetType(srcVal.Type(), path)

	switch srcVal.Kind() {
	case reflect.Pointer:
		if srcVal.IsNil() {
			return reflect.Zero(targetType), nil
		}
		elem, err := c.convertValue(srcVal.Elem(), path)
		if err != nil {
			return reflect.Value{}, err
		}
		ptr := reflect.New(targetType.Elem())
		ptr.Elem().Set(elem)
		return ptr, nil
	case reflect.Struct:
		return c.convertStruct(srcVal, path, targetType)
	case reflect.Slice:
		return c.convertSlice(srcVal, path, targetType)
	case reflect.Map:
		return c.convertMap(srcVal, path, targetType)
	default:
		return srcVal, nil
	}
}

// convertWithRule applies a rule to the value of a column it matched.
func convertWithRule(srcVal reflect.Value, rule *RetypeRule, path string) (reflect.Value, error) {
	targetType := ruleTargetType(rule, srcVal.Type())
	if srcVal.Kind() == reflect.Pointer {
		if srcVal.IsNil() {
			return reflect.Zero(targetType), nil
		}
		srcVal = srcVal.Elem()
	}
	if convertsElements(rule, srcVal.Type()) {
		if srcVal.IsNil() {
			return reflect.Zero(targetType), nil
		}
		targetSlice := reflect.MakeSlice(targetType, srcVal.Len(), srcVal.Len())
		for i := range srcVal.Len() {
			converted, err := convertWithRule(srcVal.Index(i), rule, path)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("failed to convert list element [%d]: %w", i, err)
			}
			targetSlice.Index(i).Set(converted)
		}
		return targetSlice, nil
	}

	result, err := rule.ConvertData(srcVal.Interface())
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to convert field [%s]: %w", displayPath(path), err)
	}
	if result == nil {
		return reflect.Zero(targetType), nil
	}
	resultVal := reflect.ValueOf(result)
	if targetType.Kind() != reflect.Pointer {
		return resultVal, nil
	}
	ptr := reflect.New(targetType.Elem())
	ptr.Elem().Set(resultVal)
	return ptr, nil
}

func (c *Converter) convertStruct(srcVal reflect.Value, path string, targetType reflect.Type) (reflect.Value, error) {
	targetVal := reflect.New(targetType).Elem()
	for i := range srcVal.NumField() {
		converted, err := c.convertValue(srcVal.Field(i), childPath(path, srcVal.Type().Field(i).Name))
		if err != nil {
			return reflect.Value{}, err
		}
		targetVal.Field(i).Set(converted)
	}
	return targetVal, nil
}

func (c *Converter) convertSlice(srcVal reflect.Value, path string, targetType reflect.Type) (reflect.Value, error) {
	if srcVal.IsNil() {
		return reflect.Zero(targetType), nil
	}

	elemPath := c.elemPath(path)
	targetSlice := reflect.MakeSlice(targetType, srcVal.Len(), srcVal.Len())
	for i := range srcVal.Len() {
		converted, err := c.convertValue(srcVal.Index(i), elemPath)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("failed to convert list element [%d]: %w", i, err)
		}
		targetSlice.Index(i).Set(converted)
	}
	return targetSlice, nil
}

func (c *Converter) convertMap(srcVal reflect.Value, path string, targetType reflect.Type) (reflect.Value, error) {
	if srcVal.IsNil() {
		return reflect.Zero(targetType), nil
	}

	valuePath := mapValuePath(path)
	targetMap := reflect.MakeMapWithSize(targetType, srcVal.Len())
	iter := srcVal.MapRange()
	for iter.Next() {
		converted, err := c.convertValue(iter.Value(), valuePath)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("failed to convert map value [%v]: %w", iter.Key().Interface(), err)
		}
		targetMap.SetMapIndex(iter.Key(), converted)
	}
	return targetMap, nil
}

// targetType returns the Go type a value of srcType at the column path converts to.
func (c *Converter) targetType(srcType reflect.Type, path string) reflect.Type {
	key := typeKey{srcType: srcType, path: path}
	if cached, ok := c.typeCache[key]; ok {
		return cached
	}
	targetType := c.buildTargetType(srcType, path)
	c.typeCache[key] = targetType
	return targetType
}

func (c *Converter) buildTargetType(srcType reflect.Type, path string) reflect.Type {
	if rules := c.rulesAt(path); len(rules) != 0 {
		for _, rule := range rules {
			srcType = ruleTargetType(rule, srcType)
		}
		return srcType
	}

	switch srcType.Kind() {
	case reflect.Pointer:
		return reflect.PointerTo(c.targetType(srcType.Elem(), path))
	case reflect.Struct:
		fields := make([]reflect.StructField, srcType.NumField())
		for i := range srcType.NumField() {
			fields[i] = srcType.Field(i)
			fieldPath := childPath(path, fields[i].Name)
			if c.retypes(fieldPath) {
				// a repetition tag left on a retyped field would conflict with its new shape
				fields[i].Tag = ""
			}
			fields[i].Type = c.targetType(fields[i].Type, fieldPath)
		}
		return reflect.StructOf(fields)
	case reflect.Slice:
		return reflect.SliceOf(c.targetType(srcType.Elem(), c.elemPath(path)))
	case reflect.Map:
		return reflect.MapOf(srcType.Key(), c.targetType(srcType.Elem(), mapValuePath(path)))
	default:
		return srcType
	}
}

// rulesAt returns, in the order they reshaped the schema, the rules that matched the column.
func (c *Converter) rulesAt(path string) []*RetypeRule {
	var rules []*RetypeRule
	for _, conv := range c.converters {
		if _, ok := conv.Fields[path]; ok {
			rules = append(rules, conv.Rule)
		}
	}
	return rules
}

// retypes reports whether a rule changes the Go type of the column.
func (c *Converter) retypes(path string) bool {
	for _, rule := range c.rulesAt(path) {
		if rule.TargetType != nil {
			return true
		}
	}
	return false
}

// ruleTargetType returns the Go type a rule turns a value of srcType into.
func ruleTargetType(rule *RetypeRule, srcType reflect.Type) reflect.Type {
	switch {
	case srcType.Kind() == reflect.Pointer:
		return reflect.PointerTo(ruleTargetType(rule, srcType.Elem()))
	case convertsElements(rule, srcType):
		return reflect.SliceOf(ruleTargetType(rule, srcType.Elem()))
	case rule.TargetType == nil:
		return srcType
	default:
		return rule.TargetType
	}
}

// convertsElements reports whether a rule meant for single values meets a repeated column's slice.
func convertsElements(rule *RetypeRule, srcType reflect.Type) bool {
	return srcType.Kind() == reflect.Slice && rule.InputKind != reflect.Invalid && rule.InputKind != reflect.Slice
}

// elemPath is the column path of a slice's elements: a LIST's element, or the repeated column itself.
func (c *Converter) elemPath(path string) string {
	if _, ok := c.listPaths[path]; ok {
		return childPath(childPath(path, "List"), "Element")
	}
	return path
}
