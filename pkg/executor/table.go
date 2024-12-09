package executor

import (
	"context"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
)

var (
	CreateTableID                                = "CREATE_TABLE"
	CreateTableParamsTableKey             ctxKey = "CREATE_TABLE_PARAMS_TABLE"
	CreateTableParamsCreateOrReplaceKey   ctxKey = "CREATE_TABLE_PARAMS_CREATE_OR_REPLACE"
	CreateTableParamsCreateIfNotExistsKey ctxKey = "CREATE_TABLE_PARAMS_CREATE_IF_NOT_EXISTS"
	CreateTableResponseKey                ctxKey = "CREATE_TABLE_RESPONSE"

	DropTableID                        = "DROP_TABLE"
	DropTableParamsDatabaseKey  ctxKey = "DROP_TABLE_PARAMS_DATABASE"
	DropTableParamsTableNameKey ctxKey = "DROP_TABLE_PARAMS_TABLE_NAME"
	DropTableResponseKey        ctxKey = "DROP_TABLE_RESPONSE"
)

func CreateTableAction() Action {
	return Action{
		ID: CreateTableID,
		Execute: func(rootCollection *gokvstore.Collection, in context.Context) (out context.Context, res ExecuteResult, err error) {
			table, ok := in.Value(CreateTableParamsTableKey).(ddl.Table)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(CreateTableParamsTableKey)
			}

			createOrReplace, ok := in.Value(CreateTableParamsCreateOrReplaceKey).(bool)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(CreateTableParamsCreateOrReplaceKey)
			}

			createIfNotExists, ok := in.Value(CreateTableParamsCreateIfNotExistsKey).(bool)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(CreateTableParamsCreateIfNotExistsKey)
			}

			if err := ddl.CreateTable(nil, table, createOrReplace, createIfNotExists); err != nil {
				return in, nil, err
			}

			return in, successExecutionResult(), nil
		},
	}
}

func DropTableAction() Action {
	return Action{
		ID: CreateTableID,
		Execute: func(rootCollection *gokvstore.Collection, in context.Context) (context.Context, ExecuteResult, error) {
			database, ok := in.Value(DropTableParamsDatabaseKey).(string)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(DropTableParamsDatabaseKey)
			}

			tableName, ok := in.Value(DropTableParamsTableNameKey).(string)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(DropTableParamsTableNameKey)
			}

			if err := ddl.DropTable(nil, database, tableName); err != nil {
				return in, nil, err
			}

			return in, successExecutionResult(), nil
		},
	}
}
