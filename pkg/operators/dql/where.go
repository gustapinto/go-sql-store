package dql

import (
	"errors"
	"fmt"

	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"github.com/gustapinto/go-sql-store/pkg/operators/dml"
	"github.com/gustapinto/go-sql-store/pkg/utils/stringutils"
)

type WhereFunc func(row dml.Row, column string, value any) (bool, error)

type Filter struct {
	Column  string
	Operand string
	Where   WhereFunc
	Value   any
}

var (
	FilterOperandOr     = "OR"
	FilterOperandOrNot  = "OR NOT"
	FilterOperandAnd    = "AND"
	FilterOperandAndNot = "AND NOT"
)

var (
	ErrColumnNotFound  = errors.New("column not found")
	ErrInvalidDataType = errors.New("invalid data type")
)

func WhereColumnEquals(row dml.Row, column string, value any) (bool, error) {
	for _, c := range row.Columns {
		if !stringutils.EqualsIgnoreCase(c.Definition.Name, column) {
			continue
		}

		if !ddl.ValueHasCorrectTypeForColumn(value, c.Definition) {
			return false, ErrInvalidDataType
		}

		return fmt.Sprint(value) == fmt.Sprint(c.Value), nil
	}

	return false, ErrColumnNotFound
}

func ShouldDoActionOnRow(row dml.Row, filters ...Filter) (bool, error) {
	shouldDoAction := true
	for _, filter := range filters {
		isMatch, err := filter.Where(row, filter.Column, filter.Value)
		if err != nil {
			return false, err
		}

		if filter.Operand == FilterOperandAnd {
			shouldDoAction = shouldDoAction && isMatch
			continue
		}

		if filter.Operand == FilterOperandAndNot {
			shouldDoAction = shouldDoAction && !isMatch
			continue
		}

		if filter.Operand == FilterOperandOr {
			shouldDoAction = shouldDoAction || isMatch
			continue
		}

		if filter.Operand == FilterOperandOrNot {
			shouldDoAction = shouldDoAction || !isMatch
			continue
		}
	}

	return shouldDoAction, nil
}
