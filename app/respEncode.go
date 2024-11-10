package main

import (
	"fmt"
)

func respEncodeBulkString(str string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
}

func respEncodeString(str string) string {
	return fmt.Sprintf("+%s\r\n", str)
}

