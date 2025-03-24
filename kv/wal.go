package kv

import (
	"fmt"
	"os"
)

func AppendLog(filename string, line string) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fmt.Fprintln(f, line)
}