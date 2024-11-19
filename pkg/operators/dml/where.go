package dml

import (
	"errors"
	"strings"
)

type WhereFunc func(row Row, column string, value any) (bool, error)

var (
	ErrColumnNotFound                       = errors.New("column not found")
	ErrCannotCompareWithMismatchingDataType = errors.New("cannot compare if values are equal on columns with mismatching data types")
	ErrInvalidDataType                      = errors.New("invalid data type")
)

func WhereColumnEquals(row Row, column string, value any) (bool, error) {
	for _, c := range row.Columns {
		if strings.ToUpper(c.Name) != strings.ToUpper(column) {
			continue
		}

		switch filterValue := value.(type) {
		case int64:
			columnValue, ok := c.Value.(int64)
			if !ok {
				return false, ErrCannotCompareWithMismatchingDataType
			}

			return filterValue == columnValue, nil
		case float64:
			columnValue, ok := c.Value.(float64)
			if !ok {
				return false, ErrCannotCompareWithMismatchingDataType
			}

			return filterValue == columnValue, nil
		case string:
			columnValue, ok := c.Value.(string)
			if !ok {
				return false, ErrCannotCompareWithMismatchingDataType
			}

			return filterValue == columnValue, nil
		default:
			return false, ErrInvalidDataType
		}
	}

	return false, ErrColumnNotFound
}
