package main

import (
	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/ddl"
	"log"
)

func main() {
	log.Println("Starting application...")

	root, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore("temp"))
	if err != nil {
		panic(err)
	}

	//if err := ddl.SaveDatabase(root, ddl.DatabaseDefinition{Name: "orders"}); err != nil {
	//	panic(err)
	//}
	//JJ
	//if err := ddl.SaveTable(root, ddl.TableDefinition{
	//	Name:     "orders_table",
	//	Database: "orders",
	//	Columns: []ddl.ColumnDefinition{
	//		{
	//			Name:         "id",
	//			DataType:     "INTEGER",
	//			IsPrimaryKey: true,
	//		},
	//	},
	//}); err != nil {
	//	panic(err)
	//}

	if err := ddl.DropDatabaseByName(root, "orders"); err != nil {
		panic(err)
	}
}
