package dml

import (
	"errors"
	"os"
	"testing"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

func testDeleteMockRootCollection() (*gokvstore.Collection, error) {
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
				Value: "FOO",
			},
			{
				Definition: ddl.Column{
					Name:     "DESCRIPTION",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "BAR",
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

	if err := rowCollection.Put("FOO", rowBuffer, false); err != nil {
		return nil, err
	}

	return collection, nil
}

func TestDelete(t *testing.T) {
	testCases := []struct {
		name          string
		primaryKey    string
		expectedError error
		row           Row
	}{
		{
			name:          "should delete from collection",
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
		rootCollection, err := testDeleteMockRootCollection()
		if err != nil {
			t.Errorf("not expected error when mocking root collection, got %s", err)
			return
		}
		defer rootCollection.Truncate()

		t.Run(testCase.name, func(t *testing.T) {
			if err := Delete(rootCollection, testCase.row); !errors.Is(err, testCase.expectedError) {
				t.Errorf("not expected error, got %s", err)
				return
			}

			rowCollection, err := RowCollection(rootCollection, testCase.row.Database, testCase.row.Table)
			if err != nil {
				t.Errorf("not expected error when retrieving row collection, got %s", err)
				return
			}
			defer rowCollection.Truncate()

			if exists := rowCollection.Exists(testCase.primaryKey); exists {
				t.Errorf("expected deleted row to not exist in the row collection")
			}
		})
	}
}
