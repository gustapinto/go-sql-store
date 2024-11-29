package dql

import (
	"errors"
	"os"
	"testing"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"github.com/gustapinto/go-sql-store/pkg/operators/dml"
	"github.com/gustapinto/go-sql-store/pkg/utils/encodingutils"
)

var testSelectMockedRows = []dml.Row{
	{
		Table:    "FOO_TABLE",
		Database: "FOO_DB",
		Columns: []dml.Column{
			{
				Definition: ddl.Column{
					Name:     "ID",
					DataType: ddl.ColumnDataTypeInteger,
					Constraints: []ddl.Constraint{
						{
							Type: ddl.ConstraintPrimaryKey,
							Name: "name_pk",
						},
					},
				},
				Value: int64(1),
			},
			{
				Definition: ddl.Column{
					Name:     "NAME",
					DataType: ddl.ColumnDataTypeText,
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
	},
	{
		Table:    "FOO_TABLE",
		Database: "FOO_DB",
		Columns: []dml.Column{
			{
				Definition: ddl.Column{
					Name:     "ID",
					DataType: ddl.ColumnDataTypeInteger,
					Constraints: []ddl.Constraint{
						{
							Type: ddl.ConstraintPrimaryKey,
							Name: "name_pk",
						},
					},
				},
				Value: int64(2),
			},
			{
				Definition: ddl.Column{
					Name:     "NAME",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "FOO2",
			},
			{
				Definition: ddl.Column{
					Name:     "DESCRIPTION",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "BAR2",
			},
		},
	},
}

func testSelectMockRootCollection() (*gokvstore.Collection, error) {
	collection, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore(os.TempDir()))
	if err != nil {
		return nil, err
	}

	for _, row := range testSelectMockedRows {
		rowCollection, err := dml.RowCollection(collection, row.Database, row.Table)
		if err != nil {
			return nil, err
		}

		rowBuffer, err := encodingutils.Encode(row)
		if err != nil {
			return nil, err
		}

		primaryKey, err := dml.PrimaryKeyForRow(row)
		if err != nil {
			return nil, err
		}

		if err := rowCollection.Put(primaryKey, rowBuffer, false); err != nil {
			return nil, err
		}
	}

	return collection, nil
}

func TestSelect(t *testing.T) {
	testCases := []struct {
		name          string
		expectedError error
		expectedLen   int // TODO: Improve to use a "expectedRows"
		filters       []Filter
	}{
		{
			name:          "should return every row when not using filters",
			expectedError: nil,
			expectedLen:   2,
			filters:       []Filter{},
		},
	}

	for _, testCase := range testCases {
		rootCollection, err := testSelectMockRootCollection()
		if err != nil {
			t.Errorf("not expected error when mocking root collection, got %s", err)
			return
		}
		defer rootCollection.Truncate()

		t.Run(testCase.name, func(t *testing.T) {
			actual, err := Select(rootCollection, testSelectMockedRows[0].Database, testSelectMockedRows[0].Table, testCase.filters)
			if !errors.Is(err, testCase.expectedError) {
				t.Errorf("expected %s error, got %s", testCase.expectedError.Error(), err.Error())
				return
			}

			if len(actual) != testCase.expectedLen {
				t.Errorf("expected len %v, got %v", testCase.expectedLen, len(actual))
				return
			}
		})
	}
}
