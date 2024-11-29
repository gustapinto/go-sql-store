package dql

import (
	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/dml"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

func Select(rootCollection *gokvstore.Collection, database, table string, filters []Filter) (rows []dml.Row, err error) {
	rowCollection, err := dml.RowCollection(rootCollection, database, table)
	if err != nil {
		return nil, err
	}

	for key := range rowCollection.Keys() {
		rowBuffer, err := rowCollection.Get(key)
		if err != nil {
			return nil, err
		}

		row, err := encodingutils.Decode[dml.Row](rowBuffer)
		if err != nil {
			return nil, err
		}

		shouldSelectRow, err := ShouldDoActionOnRow(row, filters...)
		if err != nil {
			return nil, err
		}

		if shouldSelectRow {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

func SelectByPrimaryKey(rootCollection *gokvstore.Collection, database, table, primaryKey string) (*dml.Row, error) {
	rowCollection, err := dml.RowCollection(rootCollection, database, table)
	if err != nil {
		return nil, err
	}

	rowBuffer, err := rowCollection.Get(primaryKey)
	if err != nil {
		return nil, err
	}

	row, err := encodingutils.Decode[dml.Row](rowBuffer)
	if err != nil {
		return nil, err
	}

	return &row, nil
}
