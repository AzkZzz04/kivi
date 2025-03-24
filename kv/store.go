package kv

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Store struct {
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Execute(cmd string) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 { // no command
		fmt.Println("Empty command")
		return
	}

	switch parts[0] {
	case "SET": // set key value
		if len(parts) != 3 {
			fmt.Println("Usage: SET key value")
			return
		}
		s.data[parts[1]] = parts[2]
		fmt.Println("set")
	case "GET": // get key
		if len(parts) != 2 {
			fmt.Println("Usage: GET key")
			return
		}
		if val, ok := s.data[parts[1]]; ok { 
            fmt.Println(val)
        } else {
            fmt.Println("Key not found")
        }
	case "DEL": // delete key
		if len(parts) != 2 {
			fmt.Println("Usage: DEL key")
			return
		}
		delete(s.data, parts[1])
		fmt.Println("deleted")
	default:
		fmt.Println("Unknown command")
	}
}

func (s *Store) Recover(log string) {
	f, err := os.Open(log)
	if err != nil {
		fmt.Println("No WAL found")
		return
	}
	defer f.Close()
	fmt.Println("Recovering from WAL")

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		s.Execute(scanner.Text())
	}

	fmt.Println("Recovery complete")

}