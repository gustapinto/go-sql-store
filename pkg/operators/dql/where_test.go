package dql

import (
	"errors"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"github.com/gustapinto/go-sql-store/pkg/operators/dml"
	"testing"
	"time"
)

func TestWhereColumnEquals(t *testing.T) {
	testCases := []struct {
		name          string
		column        string
		value         any
		row           dml.Row
		expectedValue bool
		expectedError error
	}{
		{
			name:   "should return true without errors with one row",
			column: "name",
			value:  "Foo",
			row: dml.Row{
				Columns: []dml.Column{
					{
						Definition: ddl.Column{
							Name:     "NAME",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "Foo",
					},
				},
			},
			expectedValue: true,
			expectedError: nil,
		},
		{
			name:   "should return true without errors with two rows",
			column: "value",
			value:  float64(10.0),
			row: dml.Row{
				Columns: []dml.Column{
					{
						Definition: ddl.Column{
							Name:     "NAME",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "Foo",
					},
					{
						Definition: ddl.Column{
							Name:     "VALUE",
							DataType: ddl.ColumnDataTypeFloat,
						},
						Value: float64(10.0),
					},
				},
			},
			expectedValue: true,
			expectedError: nil,
		},
		{
			name:   "should return ErrColumnNotFound when desired column does not exists in row",
			column: "foobar",
			value:  "foobar",
			row: dml.Row{
				Columns: []dml.Column{
					{
						Definition: ddl.Column{
							Name:     "ID",
							DataType: ddl.ColumnDataTypeInteger,
							Constraints: []ddl.Constraint{
								{
									Type: ddl.ConstraintPrimaryKey,
									Name: "id_pk",
								},
							},
						},
						Value: int64(123),
					},
				},
			},
			expectedValue: false,
			expectedError: ErrColumnNotFound,
		},
		{
			name:   "should return ErrInvalidDataType when value and column types does not matches",
			column: "id",
			value:  float64(123),
			row: dml.Row{
				Columns: []dml.Column{
					{
						Definition: ddl.Column{
							Name:     "ID",
							DataType: ddl.ColumnDataTypeInteger,
							Constraints: []ddl.Constraint{
								{
									Type: ddl.ConstraintPrimaryKey,
									Name: "id_pk",
								},
							},
						},
						Value: int64(123),
					},
				},
			},
			expectedValue: false,
			expectedError: ErrInvalidDataType,
		},
		{
			name:   "should return ErrInvalidDataType when value type is not supported",
			column: "timestamp",
			value:  time.Now(),
			row: dml.Row{
				Columns: []dml.Column{
					{
						Definition: ddl.Column{
							Name:     "TIMESTAMP",
							DataType: ddl.ColumnDataTypeTimestamp,
						},
						Value: time.Now().UnixMilli(),
					},
				},
			},
			expectedValue: false,
			expectedError: ErrInvalidDataType,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			value, err := WhereColumnEquals(testCase.row, testCase.column, testCase.value)
			if !errors.Is(err, testCase.expectedError) {
				t.Errorf("expected to error with %s, got %s", testCase.expectedError, err)
				return
			}

			if value != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, value)
				return
			}
		})
	}
}

func TestShouldDoActionOnRow(t *testing.T) {
	mockedRow := dml.Row{
		Columns: []dml.Column{
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
		row      dml.Row
		filters  []Filter
		expected bool
	}{
		{
			name: "should do action on row with one and filter",
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
			name: "should do action on row with two and filter",
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
			name: "should do action on row with two or filter and correct values",
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
			name: "should do action on row with two or filter and one incorrect value",
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
			name: "should do action on row with two or filter and two incorrect values",
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
			name: "should do action on row with two or not filter and two correct values",
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
			name: "should not do action on row with two and not filter and two correct values",
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
			name: "should do action on row with and not filter",
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
			actual, err := ShouldDoActionOnRow(testCase.row, testCase.filters...)
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
