package dml

import (
	"errors"
	"os"
	"testing"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
)

func TestInsert(t *testing.T) {
	testCases := []struct {
		name          string
		primaryKey    string
		expectedError error
		row           Row
	}{
		{
			name:          "should insert into collection",
			primaryKey:    "FOO",
			expectedError: nil,
			row: Row{
				Database: "FOO_DB",
				Table:    "FOO_TABLE",
				Columns: []Column{
					{
						Definition: ddl.Column{
							Name:     "NAME",
							DataType: ddl.ColumnDataTypeText,
							Constraints: []ddl.Constraint{
								{
									Type: ddl.ConstraintPrimaryKey,
									Name: "name_pk",
								},
							},
						},
						Value: "FOO",
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		rootCollection, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore(os.TempDir()))
		if err != nil {
			t.Errorf("not expected error when mocking root collection, got %s", err)
			return
		}

		t.Run(testCase.name, func(t *testing.T) {
			if err := Insert(rootCollection, testCase.row); !errors.Is(err, testCase.expectedError) {
				t.Errorf("not expected error, got %s", err)
				return
			}

			rowCollection, err := RowCollection(rootCollection, testCase.row.Database, testCase.row.Table)
			if err != nil {
				t.Errorf("not expected error when retrieving row collection, got %s", err)
				return
			}

			if exists := rowCollection.Exists(testCase.primaryKey); !exists {
				t.Errorf("expected inserted row to exists in the row collection")
			}
		})
	}
}
