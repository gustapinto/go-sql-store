package dml

import (
	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/encode"
)

func InsertInto(rootCollection *gokvstore.Collection, row Row) error {
	key, err := keyForRow(row)
	if err != nil {
		return err
	}

	rowCollection, err := RowCollection(rootCollection, row.Database, row.Table)
	if err != nil {
		return err
	}

	rowBuffer, err := encode.Encode(row)
	if err != nil {
		return err
	}

	return rowCollection.Put(key, rowBuffer, false)
}
