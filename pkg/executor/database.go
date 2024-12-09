package executor

import (
	"context"

	gokvstore "github.com/gustapinto/go-kv-store"
)

var (
	CreateDatabaseID                             = "CREATE_DATABASE"
	CreateDatabaseParamsName              ctxKey = "CREATE_DATABASE_PARAMS_DATABASE"
	CreateDatabaseParamsCreateOrReplace   ctxKey = "CREATE_DATABASE_PARAMS_CREATE_OR_REPLACE"
	CreateDatabaseParamsCreateIfNotExists ctxKey = "CREATE_DATABASE_PARAMS_CREATE_IF_NOT_EXISTS"
	CreateDatabaseResponse                ctxKey = "CREATE_DATABASE_RESPONSE"
)

func CreateDatabaseAction() Action {
	return Action{
		ID: CreateDatabaseID,
		Execute: func(rootCollection *gokvstore.Collection, in context.Context) (context.Context, ExecuteResult, error) {
			// TODO
			return in, nil, nil
		},
	}
}
