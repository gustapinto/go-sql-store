package dml

import (
	"strings"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

func Update(rootCollection *gokvstore.Collection, originalRow Row, columnsToBeUpdated map[string]any) (updated bool, err error) {
	rowCollection, err := RowCollection(rootCollection, originalRow.Database, originalRow.Table)
	if err != nil {
		return false, err
	}

	primaryKey, err := PrimaryKeyForRow(originalRow)
	if err != nil {
		return false, err
	}

	for i, column := range originalRow.Columns {
		value, exists := columnsToBeUpdated[strings.ToUpper(column.Definition.Name)]
		if exists {
			originalRow.Columns[i].Value = value
		}
	}

	newRowBuffer, err := encodingutils.Encode(originalRow)
	if err != nil {
		return false, err
	}

	if err := rowCollection.Put(primaryKey, newRowBuffer, false); err != nil {
		return false, err
	}

	return true, nil
}
