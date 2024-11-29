package main

import (
	"fmt"
)

// returns the resp bulk-string encoded value of the string provided.
func respEncodeBulkString(str string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
}

// returns the resp simple-string encoded value of the string provided.
func respEncodeString(str string) string {
	return fmt.Sprintf("+%s\r\n", str)
}

// returns a resp-array encoded string, containing bulk-string encoded values
// of the strings provided. (strs should contain un-encoded values).
func respEncodeStringArray(strs []string) string {
	arrayLength := len(strs)
	arrayString := fmt.Sprintf("*%d\r\n", arrayLength)

	for _, str := range strs {
		arrayString += respEncodeBulkString(str)
	}

	return arrayString
}

// returns the resp-encoded string format of the given int.
func respEncodeInteger(num int) string {
	return fmt.Sprintf(":%d\r\n", num)
}

// returns the resp-encoded string format of the given error message.
func respEncodeError(err string) string {
	return fmt.Sprintf("-%s\r\n", err)
}
