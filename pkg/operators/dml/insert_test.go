package dml

import (
	"errors"
	"os"
	"testing"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

func testInsertMockRootCollection() (*gokvstore.Collection, error) {
	row := Row{
		Table:    "FOO_TABLE",
		Database: "FOO_DB",
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
				Value: "EXISTING-PRIMARY-KEY",
			},
		},
	}

	collection, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore(os.TempDir()))
	if err != nil {
		return nil, err
	}

	rowCollection, err := RowCollection(collection, row.Database, row.Table)
	if err != nil {
		return nil, err
	}

	rowBuffer, err := encodingutils.Encode(row)
	if err != nil {
		return nil, err
	}

	if err := rowCollection.Put("EXISTING-PRIMARY-KEY", rowBuffer, false); err != nil {
		return nil, err
	}

	return collection, nil
}

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
		{
			name:          "should not insert into collection if primary key already exists",
			primaryKey:    "EXISTING-PRIMARY-KEY",
			expectedError: ErrPrimaryKeyAlreadyExists,
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
						Value: "EXISTING-PRIMARY-KEY",
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		rootCollection, err := testInsertMockRootCollection()
		if err != nil {
			t.Errorf("not expected error when mocking root collection, got %s", err)
			return
		}
		defer rootCollection.Truncate()

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
			defer rowCollection.Truncate()

			if exists := rowCollection.Exists(testCase.primaryKey); !exists {
				t.Errorf("expected inserted row to exists in the row collection")
			}
		})
	}
}
