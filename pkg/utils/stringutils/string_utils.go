package stringutils

import "strings"

func EqualsIgnoreCase(str1, str2 string) bool {
	str1 = strings.ToUpper(strings.TrimSpace(str1))
	str2 = strings.ToUpper(strings.TrimSpace(str2))

	return str1 == str2
}
