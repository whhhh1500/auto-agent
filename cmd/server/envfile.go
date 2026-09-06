package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode"
)

const maxEnvFileBytes = 1 << 20

// loadDotEnv loads a dotenv file without replacing deployment-injected
// environment variables. Parse errors identify only the line and error class;
// the source line may contain a secret and is never echoed.
func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open dotenv file: %w", err)
	}
	defer file.Close()

	reader := io.LimitReader(file, maxEnvFileBytes+1)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read dotenv file: %w", err)
	}
	if len(data) > maxEnvFileBytes {
		return fmt.Errorf("dotenv file exceeds %d bytes", maxEnvFileBytes)
	}

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := scanner.Text()
		if lineNumber == 1 {
			line = strings.TrimPrefix(line, "\ufeff")
		}
		key, value, present, parseErr := parseDotEnvLine(line)
		if parseErr != nil {
			return fmt.Errorf("parse dotenv line %d: %w", lineNumber, parseErr)
		}
		if !present {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set dotenv key at line %d: %w", lineNumber, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan dotenv file: %w", err)
	}
	return nil
}

func parseDotEnvLine(line string) (key, value string, present bool, err error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return "", "", false, nil
	}
	if strings.HasPrefix(trimmed, "export ") {
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "export "))
	}
	keyPart, valuePart, found := strings.Cut(trimmed, "=")
	if !found {
		return "", "", false, fmt.Errorf("missing equals sign")
	}
	key = strings.TrimSpace(keyPart)
	if !validEnvKey(key) {
		return "", "", false, fmt.Errorf("invalid key")
	}
	value, err = parseDotEnvValue(strings.TrimSpace(valuePart))
	if err != nil {
		return "", "", false, err
	}
	return key, value, true, nil
}

func validEnvKey(key string) bool {
	for index, char := range key {
		if index == 0 {
			if char != '_' && !unicode.IsLetter(char) {
				return false
			}
			continue
		}
		if char != '_' && !unicode.IsLetter(char) && !unicode.IsDigit(char) {
			return false
		}
	}
	return key != ""
}

func parseDotEnvValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if raw[0] == '\'' {
		end := strings.IndexByte(raw[1:], '\'')
		if end < 0 {
			return "", fmt.Errorf("unterminated single-quoted value")
		}
		end++
		if !validDotEnvSuffix(raw[end+1:]) {
			return "", fmt.Errorf("unexpected content after quoted value")
		}
		return raw[1:end], nil
	}
	if raw[0] == '"' {
		end, escaped := -1, false
		for index := 1; index < len(raw); index++ {
			switch {
			case escaped:
				escaped = false
			case raw[index] == '\\':
				escaped = true
			case raw[index] == '"':
				end = index
				index = len(raw)
			}
		}
		if end < 0 {
			return "", fmt.Errorf("unterminated double-quoted value")
		}
		if !validDotEnvSuffix(raw[end+1:]) {
			return "", fmt.Errorf("unexpected content after quoted value")
		}
		value, err := strconv.Unquote(raw[:end+1])
		if err != nil {
			return "", fmt.Errorf("invalid double-quoted value")
		}
		return value, nil
	}
	for index, char := range raw {
		if char == '#' && index > 0 && unicode.IsSpace(rune(raw[index-1])) {
			return strings.TrimSpace(raw[:index]), nil
		}
	}
	return strings.TrimSpace(raw), nil
}

func validDotEnvSuffix(suffix string) bool {
	trimmed := strings.TrimSpace(suffix)
	return trimmed == "" || strings.HasPrefix(trimmed, "#")
}
