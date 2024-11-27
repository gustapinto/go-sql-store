package dml

import (
	"errors"
	"fmt"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"strings"
)

type WhereFunc func(row Row, column string, value any) (bool, error)

var (
	ErrColumnNotFound  = errors.New("column not found")
	ErrInvalidDataType = errors.New("invalid data type")
)

func WhereColumnEquals(row Row, column string, value any) (bool, error) {
	for _, c := range row.Columns {
		if strings.ToUpper(c.Definition.Name) != strings.ToUpper(column) {
			continue
		}

		if !ddl.ValueHasCorrectTypeForColumn(value, c.Definition) {
			return false, ErrInvalidDataType
		}

		return fmt.Sprint(value) == fmt.Sprint(c.Value), nil
	}

	return false, ErrColumnNotFound
}
