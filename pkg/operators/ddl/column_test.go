package ddl

import (
	"testing"
)

func TestAreConstraintsEqual(t *testing.T) {
	testCases := []struct {
		name          string
		c1            Constraint
		c2            Constraint
		expectedValue bool
	}{
		{
			name: "should be equal",
			c1: Constraint{
				Type: ConstraintUnique,
				Name: "id_unique",
			},
			c2: Constraint{
				Type: ConstraintUnique,
				Name: "id_unique",
			},
			expectedValue: true,
		},
		{
			name: "should be equal even with different cases",
			c1: Constraint{
				Type: ConstraintUnique,
				Name: "id_unique",
			},
			c2: Constraint{
				Type: ConstraintUnique,
				Name: "ID_UNIQUE",
			},
			expectedValue: true,
		},
		{
			name: "should not be equal",
			c1: Constraint{
				Type: ConstraintUnique,
				Name: "id_unique",
			},
			c2: Constraint{
				Type: ConstraintUnique,
				Name: "foobar",
			},
			expectedValue: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if value := AreConstraintsEqual(testCase.c1, testCase.c2); value != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, value)
				return
			}
		})
	}
}

func TestAreColumnsEqual(t *testing.T) {
	testCases := []struct {
		name          string
		c1            Column
		c2            Column
		expectedValue bool
	}{
		{
			name: "should be equal without constraints",
			c1: Column{
				Name:     "ID",
				DataType: ColumnDataTypeText,
			},
			c2: Column{
				Name:     "ID",
				DataType: ColumnDataTypeText,
			},
			expectedValue: true,
		},
		{
			name: "should be equal with constraints",
			c1: Column{
				Name:     "ID",
				DataType: ColumnDataTypeText,
				Constraints: []Constraint{
					{
						Type: ConstraintUnique,
						Name: "id_unique",
					},
				},
			},
			c2: Column{
				Name:     "ID",
				DataType: ColumnDataTypeText,
				Constraints: []Constraint{
					{
						Type: ConstraintUnique,
						Name: "id_unique",
					},
				},
			},
			expectedValue: true,
		},
		{
			name: "should not be equal with constraints",
			c1: Column{
				Name:     "ID",
				DataType: ColumnDataTypeText,
				Constraints: []Constraint{
					{
						Type: ConstraintUnique,
						Name: "id_unique",
					},
				},
			},
			c2: Column{
				Name:     "ID",
				DataType: ColumnDataTypeText,
				Constraints: []Constraint{
					{
						Type: ConstraintUnique,
						Name: "foobar",
					},
				},
			},
			expectedValue: false,
		},
		{
			name: "should not be equal without constraints",
			c1: Column{
				Name:     "ID",
				DataType: ColumnDataTypeText,
			},
			c2: Column{
				Name:     "NAME",
				DataType: ColumnDataTypeText,
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

func TestColumnIsKey(t *testing.T) {
	testCases := []struct {
		name          string
		column        Column
		expectedValue bool
	}{
		{
			name: "should be false for column without constraints",
			column: Column{
				Name:        "name",
				DataType:    ColumnDataTypeText,
				Constraints: []Constraint{},
			},
			expectedValue: false,
		},
		{
			name: "should be false for column without PRIMARY KEY constraints",
			column: Column{
				Name:     "name",
				DataType: ColumnDataTypeText,
				Constraints: []Constraint{
					{
						Type: ConstraintUnique,
						Name: "name_unique",
					},
				},
			},
			expectedValue: false,
		},
		{
			name: "should be true for column with PRIMARY KEY constraints",
			column: Column{
				Name:     "name",
				DataType: ColumnDataTypeText,
				Constraints: []Constraint{
					{
						Type: ConstraintPrimaryKey,
						Name: "name_pk",
					},
					{
						Type: ConstraintUnique,
						Name: "name_unique",
					},
				},
			},
			expectedValue: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if value := ColumnIsKey(testCase.column); value != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, value)
				return
			}
		})
	}
}

func TestValueHasCorrectTypeForColumn(t *testing.T) {
	var testCases = []struct {
		name          string
		value         any
		column        Column
		expectedValue bool
	}{
		{
			name:  "should return true for string value and ColumnDataTypeText column",
			value: "Foo",
			column: Column{
				Name:     "name",
				DataType: ColumnDataTypeText,
			},
			expectedValue: true,
		},
		{
			name:  "should return false for int64 value and ColumnDataTypeText column",
			value: int64(123),
			column: Column{
				Name:     "name",
				DataType: ColumnDataTypeText,
			},
			expectedValue: false,
		},
		{
			name:  "should return false for invalid column type",
			value: "Foo",
			column: Column{
				Name:     "name",
				DataType: "FOOBAR",
			},
			expectedValue: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if value := ValueHasCorrectTypeForColumn(testCase.value, testCase.column); value != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, value)
				return
			}
		})
	}
}
