package ddl

import (
	"fmt"
	"slices"
	"strings"
)

type ColumnDataType string

type ConstraintDataType string

type Constraint struct {
	Type  ConstraintDataType
	Name  string
	Value string
}

type Column struct {
	Name        string
	DataType    ColumnDataType
	Constraints []Constraint
}

const (
	ColumnDataTypeText      ColumnDataType = "TEXT"
	ColumnDataTypeFloat     ColumnDataType = "FLOAT"
	ColumnDataTypeInteger   ColumnDataType = "INTEGER"
	ColumnDataTypeTimestamp ColumnDataType = "TIMESTAMP"

	ConstraintPrimaryKey ConstraintDataType = "PRIMARY_KEY"
	ConstraintUnique     ConstraintDataType = "UNIQUE"
)

func AreConstraintsEqual(c1, c2 Constraint) bool {
	return c1.Type == c2.Type &&
		strings.ToUpper(c1.Name) == strings.ToUpper(c2.Name) &&
		fmt.Sprint(c1.Value) == fmt.Sprint(c2.Value)
}

func AreColumnsEqual(c1, c2 Column) bool {
	if strings.ToUpper(c1.Name) != strings.ToUpper(c2.Name) || c1.DataType != c2.DataType {
		return false
	}

	return slices.EqualFunc(c1.Constraints, c2.Constraints, AreConstraintsEqual)
}

func ColumnIsKey(column Column) bool {
	if len(column.Constraints) == 0 {
		return false
	}

	for _, constraint := range column.Constraints {
		if constraint.Type == ConstraintPrimaryKey {
			return true
		}
	}

	return false
}

func ValueHasCorrectTypeForColumn(value any, column Column) bool {
	switch column.DataType {
	case ColumnDataTypeText:
		_, ok := value.(string)
		return ok

	case ColumnDataTypeFloat:
		_, ok := value.(float64)
		return ok

	case ColumnDataTypeInteger:
	case ColumnDataTypeTimestamp:
		_, ok := value.(int64)
		return ok
	}

	return false
}
