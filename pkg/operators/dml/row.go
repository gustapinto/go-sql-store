package dml

import (
	"errors"
	"fmt"
	"strings"

	gokvstore "github.com/gustapinto/go-kv-store"
)

type Column struct {
	Name  string
	IsKey bool
	Value any
}

type Row struct {
	Database string
	Table    string
	Columns  []Column
}

var (
	ErrRowWithoutKey = errors.New("cannot insert row without a key")
)

func rowDataDir(database, table string) string {
	builder := strings.Builder{}
	builder.WriteString("databases/")
	builder.WriteString(database)
	builder.WriteString("/tables/")
	builder.WriteString(table)
	builder.WriteString("/rows/")

	return builder.String()
}

func RowCollection(rootCollection *gokvstore.Collection, database, table string) (*gokvstore.Collection, error) {
	dataDir := rowDataDir(database, table)

	return rootCollection.NewCollection(dataDir)
}

func keyForRow(row Row) (string, error) {
	for _, column := range row.Columns {
		if column.IsKey {
			return fmt.Sprintf("%v", column.Value), nil
		}
	}

	return "", ErrRowWithoutKey
}
