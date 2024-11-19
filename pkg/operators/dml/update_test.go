package dml

import "testing"

func TestShouldUpdateRow(t *testing.T) {
	testCases := []struct {
		name     string
		row      Row
		filters  []Filter
		expected bool
	}{
		{
			name: "should update row with one and filter",
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
			row: Row{
				Columns: []Column{
					{
						Name:  "name",
						Value: "Foo",
					},
					{
						Name:  "description",
						Value: "Bar",
					},
				},
			},
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
