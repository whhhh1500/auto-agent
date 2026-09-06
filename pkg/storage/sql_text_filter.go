package storage

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxSQLTextFilterRunes = 128

func validateSQLTextFilter(name, value string) error {
	if value == "" {
		return nil
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s contains NUL", name)
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return fmt.Errorf("%s contains a control character", name)
		}
	}
	if utf8.RuneCountInString(value) > maxSQLTextFilterRunes {
		return fmt.Errorf("%s exceeds %d runes", name, maxSQLTextFilterRunes)
	}
	return nil
}
