package ddl

import (
	gokvstore "github.com/gustapinto/go-kv-store"
	"strings"
)

type ColumnDefinition struct {
	Name         string
	DataType     string
	IsPrimaryKey bool
}

type TableDefinition struct {
	Name     string
	Database string
	Columns  []ColumnDefinition
}

func tableQualifiedName(td TableDefinition) string {
	builder := strings.Builder{}
	builder.WriteString(td.Database)
	builder.WriteString(".")
	builder.WriteString(td.Name)

	return builder.String()
}

func tableDataDir(td TableDefinition) string {
	builder := strings.Builder{}
	builder.WriteString("databases/")
	builder.WriteString(td.Database)
	builder.WriteString("/tables/")
	builder.WriteString(td.Name)

	return builder.String()
}

var tableCollectionsCache = map[string]*gokvstore.Collection{}

func tableCollection(rootCollection *gokvstore.Collection, table TableDefinition) (*gokvstore.Collection, error) {
	if tableCollectionsCache == nil {
		tableCollectionsCache = map[string]*gokvstore.Collection{}
	}

	if collection, exists := tableCollectionsCache[tableQualifiedName(table)]; exists {
		return collection, nil
	}

	newCollection, err := rootCollection.NewCollection(tableDataDir(table))
	if err != nil {
		return nil, err
	}

	tableCollectionsCache[tableQualifiedName(table)] = newCollection
	return newCollection, nil
}

func CreateTable(rootCollection *gokvstore.Collection, table TableDefinition) error {
	col, err := tableCollection(rootCollection, table)
	if err != nil {
		return err
	}

	tableBuffer, err := Encode(table)
	if err != nil {
		return err
	}

	if err := col.Put(table.Name, tableBuffer, true); err != nil {
		return err
	}

	return nil
}
