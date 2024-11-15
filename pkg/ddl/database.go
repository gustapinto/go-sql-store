package ddl

import (
	"errors"
	gokvstore "github.com/gustapinto/go-kv-store"
	"strings"
)

type DatabaseDefinition struct {
	Name   string
	Tables []TableDefinition
}

func databaseDataDir(dd DatabaseDefinition) string {
	builder := strings.Builder{}
	builder.WriteString("databases/")
	builder.WriteString(dd.Name)

	return builder.String()
}

var (
	ErrDatabaseDoesNotExists = errors.New("database does not exist")
	ErrDatabaseAlreadyExists = errors.New("database already exists")

	databaseCollectionsCache = map[string]*gokvstore.Collection{}
)

func databaseCollection(rootCollection *gokvstore.Collection, database DatabaseDefinition) (*gokvstore.Collection, error) {
	if databaseCollectionsCache == nil {
		databaseCollectionsCache = map[string]*gokvstore.Collection{}
	}

	if collection, exists := databaseCollectionsCache[database.Name]; exists {
		return collection, nil
	}

	newCollection, err := rootCollection.NewCollection(databaseDataDir(database))
	if err != nil {
		return nil, err
	}

	databaseCollectionsCache[database.Name] = newCollection
	return newCollection, nil
}

func DatabaseExists(rootCollection *gokvstore.Collection, database DatabaseDefinition) (bool, error) {
	dd, err := GetDatabaseByName(rootCollection, database.Name)
	if err != nil {
		if errors.Is(err, gokvstore.ErrKeyNotFound) {
			return false, nil
		}

		return false, err
	}

	return dd != nil, nil
}

func SaveDatabase(rootCollection *gokvstore.Collection, database DatabaseDefinition) error {
	exists, err := DatabaseExists(rootCollection, database)
	if err != nil {
		return err
	}

	if exists {
		return ErrDatabaseAlreadyExists
	}

	col, err := databaseCollection(rootCollection, database)
	if err != nil {
		return err
	}

	databaseDefinition, err := Encode(database)
	if err != nil {
		return err
	}

	if err := col.Put(database.Name, databaseDefinition, true); err != nil {
		return err
	}

	return nil
}

func GetDatabaseByName(rootCollection *gokvstore.Collection, databaseName string) (*DatabaseDefinition, error) {
	col, err := databaseCollection(rootCollection, DatabaseDefinition{Name: databaseName})
	if err != nil {
		return nil, err
	}

	databaseBuffer, err := col.Get(databaseName)
	if err != nil {
		if errors.Is(err, gokvstore.ErrKeyNotFound) {
			return nil, ErrDatabaseDoesNotExists
		}

		return nil, err
	}

	database, err := Decode[DatabaseDefinition](databaseBuffer)
	if err != nil {
		return nil, err
	}

	return &database, nil
}

func DropDatabaseByName(rootCollection *gokvstore.Collection, databaseName string) error {
	col, err := databaseCollection(rootCollection, DatabaseDefinition{Name: databaseName})
	if err != nil {
		return err
	}

	if err := col.Truncate(); err != nil {
		if errors.Is(err, gokvstore.ErrKeyNotFound) {
			return ErrDatabaseDoesNotExists
		}

		return err
	}

	return nil
}
