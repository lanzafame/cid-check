package main

import (
	"fmt"
	"os"
)

func outputFile(name string, timestamp int64) (*os.File, error) {
	f, err := os.OpenFile(fmt.Sprintf("%s.%d", name, timestamp), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return f, nil
}
