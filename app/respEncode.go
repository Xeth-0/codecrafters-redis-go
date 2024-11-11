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

func respEncodeStringArray(strs []string) string {
	arrayLength := len(strs)

	arrayString := fmt.Sprintf("*%d\r\n", arrayLength)

	for _, str := range strs {
		arrayString += respEncodeBulkString(str)
	}

	return arrayString
}
