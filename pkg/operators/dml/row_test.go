package dml

import (
	"errors"
	"github.com/gustapinto/go-sql-store/pkg/operators/ddl"
	"testing"
)

func TestAreColumnsEqual(t *testing.T) {
	testCases := []struct {
		name          string
		c1            Column
		c2            Column
		expectedValue bool
	}{
		{
			name: "should be equal",
			c1: Column{
				Definition: ddl.Column{
					Name:     "ID",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "FOO",
			},
			c2: Column{
				Definition: ddl.Column{
					Name:     "ID",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "FOO",
			},
			expectedValue: true,
		},
		{
			name: "should not be equal",
			c1: Column{
				Definition: ddl.Column{
					Name:     "ID",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "FOO",
			},
			c2: Column{
				Definition: ddl.Column{
					Name:     "NAME",
					DataType: ddl.ColumnDataTypeText,
				},
				Value: "FOOBAR",
			},
			expectedValue: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if value := AreColumnsEqual(testCase.c1, testCase.c2); value != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, value)
				return
			}
		})
	}
}

func TestAreRowsEqual(t *testing.T) {
	testCases := []struct {
		name          string
		r1            Row
		r2            Row
		expectedValue bool
	}{
		{
			name: "should be equal",
			r1: Row{
				Database: "FOO",
				Table:    "BAR",
				Columns: []Column{
					{
						Definition: ddl.Column{
							Name:     "ID",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "FOO",
					},
					{
						Definition: ddl.Column{
							Name:     "NAME",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "FOOBAR",
					},
				},
			},
			r2: Row{
				Database: "FOO",
				Table:    "BAR",
				Columns: []Column{
					{
						Definition: ddl.Column{
							Name:     "ID",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "FOO",
					},
					{
						Definition: ddl.Column{
							Name:     "NAME",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "FOOBAR",
					},
				},
			},
			expectedValue: true,
		},
		{
			name: "should not be equal",
			r1: Row{
				Database: "FOO",
				Table:    "BAR",
				Columns: []Column{
					{
						Definition: ddl.Column{
							Name:     "ID",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "FOO",
					},
					{
						Definition: ddl.Column{
							Name:     "NAME",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "FOOBAR",
					},
				},
			},
			r2: Row{
				Database: "FOO",
				Table:    "BAR",
				Columns: []Column{
					{
						Definition: ddl.Column{
							Name:     "ID",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "LOLOLO",
					},
					{
						Definition: ddl.Column{
							Name:     "NAME",
							DataType: ddl.ColumnDataTypeText,
						},
						Value: "FOOBAR",
					},
				},
			},
			expectedValue: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if value := AreRowsEqual(testCase.r1, testCase.r2); value != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, value)
				return
			}
		})
	}
}

func TestKeyForRow(t *testing.T) {
	testCases := []struct {
		name          string
		row           Row
		expectedValue string
		expectedError error
	}{
		{
			name: "should return ErrRowWithoutKey on a Row without keys",
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
			expectedValue: "",
			expectedError: ErrRowWithoutKey,
		},
		{
			name: "should return ErrRowWithoutKey on a Row without columns",
			row: Row{
				Columns: []Column{},
			},
			expectedValue: "",
			expectedError: ErrRowWithoutKey,
		},
		{
			name: "should return the first key value on a Row with key",
			row: Row{
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
			},
			expectedValue: "Foo",
			expectedError: nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			value, err := keyForRow(testCase.row)
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
