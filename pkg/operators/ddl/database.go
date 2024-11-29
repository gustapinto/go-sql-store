package ddl

import (
	"errors"
	"strings"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

type Database struct {
	Name   string
	Tables []Table
}

var (
	ErrDatabaseDoesNotExists = errors.New("database does not exist")
	ErrDatabaseAlreadyExists = errors.New("database already exists")

	databaseCollectionsCache = map[string]*gokvstore.Collection{}
)

func databaseDataDir(dd Database) string {
	builder := strings.Builder{}
	builder.WriteString("databases/")
	builder.WriteString(dd.Name)

	return builder.String()
}

func DatabaseCollection(rootCollection *gokvstore.Collection, database Database) (*gokvstore.Collection, error) {
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

func putDatabase(rootCollection *gokvstore.Collection, database Database, replace bool) error {
	databaseCollection, err := DatabaseCollection(rootCollection, database)
	if err != nil {
		return err
	}

	if replace {
		if err := databaseCollection.Truncate(); err != nil {
			return err
		}
	}

	databaseBuffer, err := encodingutils.Encode(database)
	if err != nil {
		return err
	}

	if err := databaseCollection.Put(database.Name, databaseBuffer, true); err != nil {
		return err
	}

	return nil
}

func GetDatabase(rootCollection *gokvstore.Collection, databaseName string) (*Database, error) {
	databaseCollection, err := DatabaseCollection(rootCollection, Database{Name: databaseName})
	if err != nil {
		return nil, err
	}

	databaseBuffer, err := databaseCollection.Get(databaseName)
	if err != nil {
		if errors.Is(err, gokvstore.ErrKeyNotFound) {
			return nil, ErrDatabaseDoesNotExists
		}

		return nil, err
	}

	database, err := encodingutils.Decode[Database](databaseBuffer)
	if err != nil {
		return nil, err
	}

	return &database, nil
}

func DatabaseExists(rootCollection *gokvstore.Collection, database Database) (bool, error) {
	databaseCollection, err := DatabaseCollection(rootCollection, database)
	if err != nil {
		return false, err
	}

	return databaseCollection.Exists(database.Name), nil
}

func CreateDatabase(rootCollection *gokvstore.Collection, database Database, createOrReplace, createIfNotExists bool) error {
	exists, err := DatabaseExists(rootCollection, database)
	if err != nil {
		return err
	}

	canIgnoreIfExists := createOrReplace || createIfNotExists
	if exists && !canIgnoreIfExists {
		return ErrDatabaseAlreadyExists
	}

	return putDatabase(rootCollection, database, createOrReplace)
}

func AlterDatabase(rootCollection *gokvstore.Collection, database Database) error {
	exists, err := DatabaseExists(rootCollection, database)
	if err != nil {
		return err
	}

	if !exists {
		return ErrDatabaseDoesNotExists
	}

	return putDatabase(rootCollection, database, false)
}

func DropDatabase(rootCollection *gokvstore.Collection, name string) error {
	exists, err := DatabaseExists(rootCollection, Database{Name: name})
	if err != nil {
		return err
	}

	if !exists {
		return ErrDatabaseDoesNotExists
	}

	databaseCollection, err := DatabaseCollection(rootCollection, Database{Name: name})
	if err != nil {
		return err
	}

	return databaseCollection.Truncate()
}
