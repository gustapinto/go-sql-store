package dml

import (
	"errors"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"testing"
	"time"
)

func TestWhereColumnEquals(t *testing.T) {
	testCases := []struct {
		name          string
		column        string
		value         any
		row           Row
		expectedValue bool
		expectedError error
	}{
		{
			name:   "should return true without errors with one row",
			column: "name",
			value:  "Foo",
			row: Row{
				Columns: []Column{
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
			row: Row{
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
			row: Row{
				Columns: []Column{
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
			row: Row{
				Columns: []Column{
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
			row: Row{
				Columns: []Column{
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
