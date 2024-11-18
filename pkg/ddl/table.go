package ddl

import (
	"errors"
	"strings"

	gokvstore "github.com/gustapinto/go-kv-store"
)

type Table struct {
	Name     string
	Database string
	Columns  []Column
}

var (
	ErrTableDoesNotExists = errors.New("table does not exists")
	ErrTableAlreadyExists = errors.New("table already exists")

	tableCollectionsCache = map[string]*gokvstore.Collection{}
)

func tableQualifiedName(database, name string) string {
	builder := strings.Builder{}
	builder.WriteString(database)
	builder.WriteString(".")
	builder.WriteString(name)

	return builder.String()
}

func tableDataDir(database, name string) string {
	builder := strings.Builder{}
	builder.WriteString("databases/")
	builder.WriteString(database)
	builder.WriteString("/tables/")
	builder.WriteString(name)

	return builder.String()
}

func tableCollection(rootCollection *gokvstore.Collection, database, name string) (*gokvstore.Collection, error) {
	if tableCollectionsCache == nil {
		tableCollectionsCache = map[string]*gokvstore.Collection{}
	}

	if collection, exists := tableCollectionsCache[tableQualifiedName(database, name)]; exists {
		return collection, nil
	}

	newCollection, err := rootCollection.NewCollection(tableDataDir(database, name))
	if err != nil {
		return nil, err
	}

	tableCollectionsCache[tableQualifiedName(database, name)] = newCollection
	return newCollection, nil
}

func putTable(rootCollection *gokvstore.Collection, table Table, replace bool) error {
	tableCollection, err := tableCollection(rootCollection, table.Database, table.Name)
	if err != nil {
		return err
	}

	if replace {
		if err := tableCollection.Truncate(); err != nil {
			return err
		}
	}

	tableBuffer, err := Encode(table)
	if err != nil {
		return err
	}

	if err := tableCollection.Put(tableQualifiedName(table.Database, table.Name), tableBuffer, true); err != nil {
		return err
	}

	return nil
}

func GetTable(rootCollection *gokvstore.Collection, database, name string) (*Table, error) {
	tableCollection, err := tableCollection(rootCollection, database, name)
	if err != nil {
		return nil, err
	}

	tableBuffer, err := tableCollection.Get(tableQualifiedName(database, name))
	if err != nil {
		if errors.Is(err, gokvstore.ErrKeyNotFound) {
			return nil, ErrTableDoesNotExists
		}

		return nil, err
	}

	table, err := Decode[Table](tableBuffer)
	if err != nil {
		return nil, err
	}

	return &table, nil
}

func TableExists(rootCollection *gokvstore.Collection, database, name string) (bool, error) {
	tableCollection, err := tableCollection(rootCollection, database, name)
	if err != nil {
		return false, err
	}

	return tableCollection.Exists(tableQualifiedName(database, name)), nil
}

func CreateTable(rootCollection *gokvstore.Collection, table Table, createOrReplace, createIfNotExists bool) error {
	exists, err := TableExists(rootCollection, table.Database, table.Name)
	if err != nil {
		return err
	}

	canIgnoreIfExists := createOrReplace || createIfNotExists
	if exists && !canIgnoreIfExists {
		return ErrTableAlreadyExists
	}

	return putTable(rootCollection, table, createOrReplace)
}

func AlterTable(rootCollection *gokvstore.Collection, table Table) error {
	exists, err := TableExists(rootCollection, table.Database, table.Name)
	if err != nil {
		return err
	}

	if !exists {
		return ErrTableDoesNotExists
	}

	return putTable(rootCollection, table, false)
}

func DropTable(rootCollection *gokvstore.Collection, database, name string) error {
	exists, err := TableExists(rootCollection, database, name)
	if err != nil {
		return err
	}

	if !exists {
		return ErrTableDoesNotExists
	}

	tableCollection, err := tableCollection(rootCollection, database, name)
	if err != nil {
		return err
	}

	return tableCollection.Truncate()
}
