package stringutils

import "testing"

func TestEqualsIgnoreCase(t *testing.T) {
	testCases := []struct {
		name          string
		str1          string
		str2          string
		expectedValue bool
	}{
		{
			name:          "equal strings should be equal",
			str1:          "NAME",
			str2:          "NAME",
			expectedValue: true,
		},
		{
			name:          "different strings should not be equal",
			str1:          "NAME",
			str2:          "FOOBAR",
			expectedValue: false,
		},
		{
			name:          "equal strings with different cases should be equal",
			str1:          "NAME",
			str2:          "name",
			expectedValue: true,
		},
		{
			name:          "equal strings with whitespace should be equal",
			str1:          "    NAME  ",
			str2:          " name        ",
			expectedValue: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := EqualsIgnoreCase(testCase.str1, testCase.str2)
			if actual != testCase.expectedValue {
				t.Errorf("expected value %v, got %v", testCase.expectedValue, actual)
				return
			}
		})
	}
}
