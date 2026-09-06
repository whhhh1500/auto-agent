package execution

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	maxExecArgs             = 64
	maxExecArgBytes         = 8 << 10
	maxLocalExecOutputBytes = 1 << 20
	maxWasmModuleBytes      = 32 << 20
)

func validateLocalExecPath(path string) error {
	if strings.TrimSpace(path) == "" || strings.ContainsRune(path, 0) {
		return fmt.Errorf("execution path is invalid")
	}
	cleaned := filepath.Clean(path)
	sep := string(filepath.Separator)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+sep) {
		return fmt.Errorf("execution path must not contain parent segments")
	}
	return nil
}

func validateExecArgs(args []string) error {
	if len(args) > maxExecArgs {
		return fmt.Errorf("execution argument list exceeds %d entries", maxExecArgs)
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("execution argument contains NUL")
		}
		if len(arg) > maxExecArgBytes {
			return fmt.Errorf("execution argument exceeds %d bytes", maxExecArgBytes)
		}
	}
	return nil
}

// argsToStrs extracts a string slice from a capability's input map. It supports
// `inputs: [..]` (list) and `input: "..."` (single string). Nested objects are
// refused instead of being fmt.Sprint'ed onto argv.
func argsToStrs(args map[string]any) ([]string, error) {
	if v, ok := args["inputs"].([]any); ok {
		if len(v) > maxExecArgs {
			return nil, fmt.Errorf("execution argument list exceeds %d entries", maxExecArgs)
		}
		out := make([]string, 0, len(v))
		for _, x := range v {
			item, err := encodeExecArg(x)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, validateExecArgs(out)
	}
	if v, ok := args["input"]; ok && v != nil {
		item, err := encodeExecArg(v)
		if err != nil {
			return nil, err
		}
		if item == "" {
			return nil, nil
		}
		out := []string{item}
		return out, validateExecArgs(out)
	}
	return nil, nil
}

func encodeExecArg(value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		return "", nil
	case string:
		return typed, nil
	case bool:
		return strconv.FormatBool(typed), nil
	case int:
		return strconv.Itoa(typed), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case json.Number:
		return typed.String(), nil
	default:
		return "", fmt.Errorf("execution argument must be a scalar")
	}
}

type limitedBuffer struct {
	buf      []byte
	limit    int
	exceeded bool
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if l.exceeded {
		return 0, fmt.Errorf("execution output exceeds %d bytes", l.limit)
	}
	if len(l.buf)+len(p) > l.limit {
		l.exceeded = true
		return 0, fmt.Errorf("execution output exceeds %d bytes", l.limit)
	}
	l.buf = append(l.buf, p...)
	return len(p), nil
}

func (l *limitedBuffer) String() string {
	return string(l.buf)
}

func (l *limitedBuffer) err() error {
	if l != nil && l.exceeded {
		return fmt.Errorf("execution output exceeds %d bytes", l.limit)
	}
	return nil
}
