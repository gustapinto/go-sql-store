package dml

import gokvstore "github.com/gustapinto/go-kv-store"

func Delete(rootCollection *gokvstore.Collection, row Row) error {
	rowCollection, err := RowCollection(rootCollection, row.Database, row.Table)
	if err != nil {
		return err
	}

	primaryKey, err := primaryKeyForRow(row)
	if err != nil {
		return err
	}

	return rowCollection.Delete(primaryKey)
}
