package dml

import (
	"errors"
	"fmt"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"slices"
	"strings"

	gokvstore "github.com/gustapinto/go-kv-store"
)

type Column struct {
	Definition ddl.Column
	Value      any
}

type Row struct {
	Database string
	Table    string
	Columns  []Column
}

var (
	ErrRowWithoutKey = errors.New("cannot insert row without a key")
)

func AreColumnsEqual(c1, c2 Column) bool {
	if fmt.Sprint(c1.Value) != fmt.Sprint(c2.Value) {
		return false
	}

	return ddl.AreColumnsEqual(c1.Definition, c2.Definition)
}

func AreRowsEqual(r1, r2 Row) bool {
	if strings.ToUpper(r1.Database) != strings.ToUpper(r2.Database) ||
		strings.ToUpper(r1.Table) != strings.ToUpper(r2.Table) ||
		len(r1.Columns) != len(r2.Columns) {

		return false
	}

	return slices.EqualFunc(r1.Columns, r2.Columns, AreColumnsEqual)
}

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
		if ddl.ColumnIsKey(column.Definition) {
			return fmt.Sprintf("%v", column.Value), nil
		}
	}

	return "", ErrRowWithoutKey
}
