// Command calc is a tiny WASI module used to demonstrate an embedded WASM
// "computation" capability: it sums its integer args and writes the result to
// stdout. Built with:
//
//	GOOS=wasip1 GOARCH=wasm go build -o calc.wasm ./internal/calc
package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	total := 0
	for _, a := range os.Args[1:] {
		if n, err := strconv.Atoi(a); err == nil {
			total += n
		}
	}
	_, _ = fmt.Print(total)
}
