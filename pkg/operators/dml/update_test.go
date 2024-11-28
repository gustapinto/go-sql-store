package dml

import (
	"os"
	"slices"
	"testing"

	gokvstore "github.com/gustapinto/go-kv-store"
	"github.com/gustapinto/go-sql-store/pkg/encode"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
)

func TestShouldUpdateRow(t *testing.T) {
	mockedRow := Row{
		Columns: []Column{
			{
				Definition: ddl.Column{
					Name:     "NAME",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "Foo",
			},
			{
				Definition: ddl.Column{
					Name:     "DESCRIPTION",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "Bar",
			},
		},
	}

	testCases := []struct {
		name     string
		row      Row
		filters  []Filter
		expected bool
	}{
		{
			name: "should update row with one and filter",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foo",
				},
			},
			expected: true,
		},
		{
			name: "should not update row with one and filter",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foobar",
				},
			},
			expected: false,
		},
		{
			name: "should update row with two and filter",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foo",
				},
				{
					Column:  "description",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Bar",
				},
			},
			expected: true,
		},
		{
			name: "should not update row with two and filter",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foo",
				},
				{
					Column:  "description",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foobar",
				},
			},
			expected: false,
		},
		{
			name: "should update row with two or filter and correct values",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foo",
				},
				{
					Column:  "description",
					Operand: FilterOperandOr,
					Where:   WhereColumnEquals,
					Value:   "Bar",
				},
			},
			expected: true,
		},
		{
			name: "should update row with two or filter and one incorrect value",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foo",
				},
				{
					Column:  "description",
					Operand: FilterOperandOr,
					Where:   WhereColumnEquals,
					Value:   "Foobar",
				},
			},
			expected: true,
		},
		{
			name: "should update row with two or filter and two incorrect values",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foobar",
				},
				{
					Column:  "description",
					Operand: FilterOperandOr,
					Where:   WhereColumnEquals,
					Value:   "Foobar",
				},
			},
			expected: false,
		},
		{
			name: "should update row with two or not filter and two correct values",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foo",
				},
				{
					Column:  "description",
					Operand: FilterOperandOrNot,
					Where:   WhereColumnEquals,
					Value:   "Bar",
				},
			},
			expected: true,
		},
		{
			name: "should not update row with two and not filter and two correct values",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "Foo",
				},
				{
					Column:  "description",
					Operand: FilterOperandAndNot,
					Where:   WhereColumnEquals,
					Value:   "Bar",
				},
			},
			expected: false,
		},
		{
			name: "should update row with and not filter",
			row:  mockedRow,
			filters: []Filter{
				{
					Column:  "name",
					Operand: FilterOperandAndNot,
					Where:   WhereColumnEquals,
					Value:   "Foobar",
				},
			},
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := shouldUpdateRow(testCase.row, testCase.filters)
			if err != nil {
				t.Errorf("not expected error, got %s", err)
				return
			}

			if actual != testCase.expected {
				t.Errorf("expected value %v, got %v", testCase.expected, actual)
				return
			}

		})
	}
}

func mockRootCollection(row Row) (*gokvstore.Collection, error) {
	collection, err := gokvstore.NewCollection(gokvstore.NewFsRecordStore(os.TempDir()))
	if err != nil {
		return nil, err
	}

	rowCollection, err := RowCollection(collection, row.Database, row.Table)
	if err != nil {
		return nil, err
	}

	rowBuffer, err := encode.Encode(row)
	if err != nil {
		return nil, err
	}

	if err := rowCollection.Put("FOO", rowBuffer, false); err != nil {
		return nil, err
	}

	return collection, nil
}

func TestUpdateFrom(t *testing.T) {
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
		name                string
		originalRow         Row
		columnsToBeUpdated  map[string]any
		filters             []Filter
		expectedUpdatedRows []Row
		expectedError       error
	}{
		{
			name:        "should update row with filter",
			originalRow: mockedOriginalRow,
			columnsToBeUpdated: map[string]any{
				"NAME": "FOOBAR",
			},
			filters: []Filter{
				{
					Column:  "NAME",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "FOO",
				},
			},
			expectedUpdatedRows: []Row{
				{
					Database: mockedOriginalRow.Database,
					Table:    mockedOriginalRow.Table,
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
							Value: "FOOBAR",
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
			},
			expectedError: nil,
		},
		{
			name:        "should not update row when filter does not matches",
			originalRow: mockedOriginalRow,
			columnsToBeUpdated: map[string]any{
				"NAME": "FOOBAR",
			},
			filters: []Filter{
				{
					Column:  "NAME",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "FOO",
				},
				{
					Column:  "DESCRIPTION",
					Operand: FilterOperandAnd,
					Where:   WhereColumnEquals,
					Value:   "FOO",
				},
			},
			expectedUpdatedRows: []Row{},
			expectedError:       nil,
		},
	}

	for _, testCase := range testCases {
		rootCollection, err := mockRootCollection(mockedOriginalRow)
		if err != nil {
			t.Errorf("not expected error when mocking root collection, got %s", err)
			return
		}

		t.Run(testCase.name, func(t *testing.T) {
			actual, err := UpdateFrom(rootCollection, testCase.originalRow, testCase.columnsToBeUpdated, testCase.filters)
			if err != nil {
				t.Errorf("not expected error, got %s", err)
				return
			}

			if !slices.EqualFunc(testCase.expectedUpdatedRows, actual, AreRowsEqual) {
				t.Errorf("expected value %v, got %v", testCase.expectedUpdatedRows, actual)
				return
			}

		})
	}
}

func TestUpdateFromByPrimaryKey(t *testing.T) {
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
		primaryKey         string
		expectedUpdatedRow *Row
		expectedError      error
	}{
		{
			name:        "should update row with filter",
			originalRow: mockedOriginalRow,
			columnsToBeUpdated: map[string]any{
				"NAME": "FOOBAR",
			},
			primaryKey: "FOO",
			expectedUpdatedRow: &Row{
				Database: mockedOriginalRow.Database,
				Table:    mockedOriginalRow.Table,
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
						Value: "FOOBAR",
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
			expectedError: nil,
		},
		{
			name:        "should not update row when filter does not matches",
			originalRow: mockedOriginalRow,
			columnsToBeUpdated: map[string]any{
				"NAME": "FOOBAR",
			},
			primaryKey:         "FOOBAR",
			expectedUpdatedRow: nil,
			expectedError:      nil,
		},
	}

	for _, testCase := range testCases {
		rootCollection, err := mockRootCollection(mockedOriginalRow)
		if err != nil {
			t.Errorf("not expected error when mocking root collection, got %s", err)
			return
		}

		t.Run(testCase.name, func(t *testing.T) {
			actual, err := UpdateFromByPrimaryKey(rootCollection, testCase.originalRow, testCase.columnsToBeUpdated, testCase.primaryKey)
			if err != nil {
				t.Errorf("not expected error, got %s", err)
				return
			}

			if actual == nil && testCase.expectedUpdatedRow != nil {
				t.Errorf("expected value %v, got <nil>", testCase.expectedUpdatedRow)
				return
			}

			if !AreRowsEqual(*actual, *testCase.expectedUpdatedRow) {
				t.Errorf("expected value %v, got %v", testCase.expectedUpdatedRow, actual)
				return
			}
		})
	}
}
