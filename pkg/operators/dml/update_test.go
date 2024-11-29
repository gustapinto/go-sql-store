package dml

import (
	"os"
	"testing"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

func testUpdateMockRootCollection(row Row) (*gokvstore.Collection, error) {
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

func TestUpdate(t *testing.T) {
	mockedOriginalRow := Row{
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

	testCases := []struct {
		name               string
		originalRow        Row
		columnsToBeUpdated map[string]any
		expectedValue      bool
		expectedError      error
	}{
		{
			name:        "should update row with filter",
			originalRow: mockedOriginalRow,
			columnsToBeUpdated: map[string]any{
				"NAME": "FOOBAR",
			},
			expectedValue: true,
			expectedError: nil,
		},
		{
			name:        "should not update row when columns does not matches",
			originalRow: mockedOriginalRow,
			columnsToBeUpdated: map[string]any{
				"ID": "FOOBAR",
			},
			expectedValue: true,
			expectedError: nil,
		},
	}

	for _, testCase := range testCases {
		rootCollection, err := testUpdateMockRootCollection(mockedOriginalRow)
		if err != nil {
			t.Errorf("not expected error when mocking root collection, got %s", err)
			return
		}

		t.Run(testCase.name, func(t *testing.T) {
			actual, err := Update(rootCollection, testCase.originalRow, testCase.columnsToBeUpdated)
			if err != nil {
				t.Errorf("not expected error, got %s", err)
				return
			}

			if actual != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, actual)
				return
			}
		})
	}
}
