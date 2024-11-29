package dml

import (
	"errors"
	"strings"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

var (
	ErrPrimaryKeyAlreadyExists = errors.New("primary key already exists in database")
)

func Insert(rootCollection *gokvstore.Collection, row Row) error {
	primaryKey, err := primaryKeyForRow(row)
	if err != nil {
		return err
	}

	for i, column := range row.Columns {
		row.Columns[i].Definition.Name = strings.ToUpper(column.Definition.Name)
	}

	rowCollection, err := RowCollection(rootCollection, row.Database, row.Table)
	if err != nil {
		return err
	}

	if exists := rowCollection.Exists(primaryKey); exists {
		return ErrPrimaryKeyAlreadyExists
	}

	rowBuffer, err := encodingutils.Encode(row)
	if err != nil {
		return err
	}

	return rowCollection.Put(primaryKey, rowBuffer, false)
}
