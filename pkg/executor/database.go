package executor

import (
	"context"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
)

var (
	CreateDatabaseID                             = "CREATE_DATABASE"
	CreateDatabaseParamsDatabaseName      ctxKey = "CREATE_DATABASE_PARAMS_DATABASE_NAME"
	CreateDatabaseParamsCreateOrReplace   ctxKey = "CREATE_DATABASE_PARAMS_CREATE_OR_REPLACE"
	CreateDatabaseParamsCreateIfNotExists ctxKey = "CREATE_DATABASE_PARAMS_CREATE_IF_NOT_EXISTS"
	CreateDatabaseResponse                ctxKey = "CREATE_DATABASE_RESPONSE"
)

func CreateDatabaseAction() Action {
	return Action{
		ID: CreateDatabaseID,
		Execute: func(rootCollection *gokvstore.Collection, in context.Context) (context.Context, ExecuteResult, error) {
			name, ok := in.Value(CreateDatabaseParamsDatabaseName).(string)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(CreateDatabaseParamsDatabaseName)
			}

			createOrReplace, ok := in.Value(CreateDatabaseParamsCreateOrReplace).(bool)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(CreateDatabaseParamsCreateOrReplace)
			}

			createIfNotExists, ok := in.Value(CreateDatabaseParamsCreateIfNotExists).(bool)
			if !ok {
				return in, nil, valueMissingOrWithWrongTypeError(CreateDatabaseParamsCreateIfNotExists)
			}

			database := ddl.Database{Name: name}

			if err := ddl.CreateDatabase(nil, database, createOrReplace, createIfNotExists); err != nil {
				return in, nil, err
			}

			return in, successExecutionResult(), nil
		},
	}
}
