package main

import (
	"bufio"
	"fmt"
	"kivi/kv"
	"os"
	"strings"
)

func main() {
	store := kv.NewStore()
	store.Recover("wal.log")

	logFile, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening WAL file:", err)
		return
	}
	defer logFile.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("kivi> ")

		line, _ := reader.ReadString('\n')
		cmd := strings.TrimSpace(line)

		if cmd == "EXIT" {
			fmt.Println("Bye!")
			break
		}

		store.Execute(cmd)


		if cmdParts := strings.Fields(cmd); len(cmdParts) > 0 && cmdParts[0] != "GET" {
			if _, err := fmt.Fprintln(logFile, cmd); err != nil {
				fmt.Println("Error writing to WAL:", err)
			}
		}
	}
}