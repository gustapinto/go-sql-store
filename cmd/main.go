package main

import (
	"log"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/ddl"
	"github.com/gustapinto/go-sql-store/pkg/dml"
)

var exampleDatabase = ddl.Database{
	Name: "orders",
}
var exampleTable = ddl.Table{
	Name:     "orders_table",
	Database: exampleDatabase.Name,
	Columns: []ddl.Column{
		{
			Name:     "id",
			DataType: ddl.ColumnDataTypeInteger,
			Constraints: []ddl.Constraint{
				{
					Type: ddl.ConstraintPrimaryKey,
					Name: "id_pkey",
				},
			},
		},
		{
			Name:     "name",
			DataType: ddl.ColumnDataTypeText,
			Constraints: []ddl.Constraint{
				{
					Type: ddl.ConstranintUnique,
					Name: "name_unique",
				},
			},
		},
	},
}

func main() {
	log.Println("Starting application...")

	root, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore("temp"))
	if err != nil {
		panic(err)
	}

	if err := ddl.CreateDatabase(root, exampleDatabase, false, true); err != nil {
		panic(err)
	}

	if err := ddl.CreateTable(root, exampleTable, false, true); err != nil {
		panic(err)
	}

	err = dml.InsertInto(root, dml.Row{
		Database: "orders",
		Table:    "orders_table",
		Columns: []dml.Column{
			{
				Name:  "id",
				IsKey: true,
				Value: 123,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	// if err := ddl.DropDatabase(root, "orders"); err != nil {
	// 	panic(err)
	// }
}
