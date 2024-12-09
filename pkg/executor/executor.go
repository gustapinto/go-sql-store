package executor

import (
	"context"
	"fmt"
	"log/slog"

	gokvstore "github.com/gustapinto/go-kv-store"
)

type ctxKey string

type Action struct {
	ID      string
	Execute func(rootCollection *gokvstore.Collection, in context.Context) (out context.Context, result ExecuteResult, err error)
}

type ExecutionPlan struct {
	ID      string
	Actions []Action
}

type ExecuteResult map[string]any

func successExecutionResult() ExecuteResult {
	return ExecuteResult{"Status": "SUCCESS"}
}

func errorExecutionResult(err error) ExecuteResult {
	return ExecuteResult{"Status": "ERROR", "Error": err.Error()}
}

var (
	ExecutionResultKey ctxKey = "EXECUTION_RESULT"
	ExecutionIDKey     ctxKey = "EXECUTION_ID"
)

func valueMissingOrWithWrongTypeError(key ctxKey) error {
	return fmt.Errorf("value missing or with wrong type %s", string(key))
}

func Execute(rootCollection *gokvstore.Collection, plan ExecutionPlan, ctx context.Context) ExecuteResult {
	logger := slog.Default().With("actionPlan.ID", plan.ID)
	ctx = context.WithValue(ctx, ExecutionIDKey, plan.ID)

	var lastResult ExecuteResult
	for i, action := range plan.Actions {
		logger.Info("Executing", "action.Index", i, "action.ID", action.ID, "execution.Status", "Started")

		out, res, err := action.Execute(rootCollection, ctx)
		if err != nil {
			logger.Info("Executing", "action.Index", i, "action.ID", action.ID, "execution.Status", "Failed", "error", err.Error())
			return errorExecutionResult(err)
		}
		ctx = out
		lastResult = res

		logger.Info("Executing", "action.Index", i, "action.ID", action.ID, "execution.Status", "Success")
	}

	return lastResult
}
