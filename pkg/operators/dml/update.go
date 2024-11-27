package dml

import (
	"strings"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/encode"
)

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

func shouldUpdateRow(row Row, filters []Filter) (bool, error) {
	shouldUpdate := true
	for _, filter := range filters {
		isMatch, err := filter.Where(row, filter.Column, filter.Value)
		if err != nil {
			return false, err
		}

		if filter.Operand == FilterOperandAnd {
			shouldUpdate = shouldUpdate && isMatch
			continue
		}

		if filter.Operand == FilterOperandAndNot {
			shouldUpdate = shouldUpdate && !isMatch
			continue
		}

		if filter.Operand == FilterOperandOr {
			shouldUpdate = shouldUpdate || isMatch
			continue
		}

		if filter.Operand == FilterOperandOrNot {
			shouldUpdate = shouldUpdate || !isMatch
			continue
		}
	}

	return shouldUpdate, nil
}

func UpdateFrom(rootCollection *gokvstore.Collection, originalRow Row, columnsToBeUpdated map[string]any, filters []Filter) ([]Row, error) {
	rowCollection, err := RowCollection(rootCollection, originalRow.Database, originalRow.Table)
	if err != nil {
		return nil, err
	}

	var updatedRows []Row
	for key := range rowCollection.Keys() {
		rowBuffer, err := rootCollection.Get(key)
		if err != nil {
			return nil, nil
		}

		row, err := encode.Decode[Row](rowBuffer)
		if err != nil {
			return nil, err
		}

		shouldUpdate, err := shouldUpdateRow(row, filters)
		if !shouldUpdate {
			continue
		}

		newRow := Row{
			Database: originalRow.Database,
			Table:    originalRow.Table,
			Columns:  make([]Column, len(originalRow.Columns)),
		}
		for i, column := range newRow.Columns {
			value, exists := columnsToBeUpdated[strings.ToUpper(column.Definition.Name)]
			if exists {
				newRow.Columns[i] = Column{
					Definition: column.Definition,
					Value:      value,
				}
			}
		}

		newRowBuffer, err := encode.Encode(newRow)
		if err != nil {
			return nil, err
		}

		if err := rowCollection.Put(key, newRowBuffer, false); err != nil {
			return nil, err
		}

		updatedRows = append(updatedRows, newRow)
	}

	return updatedRows, nil
}
