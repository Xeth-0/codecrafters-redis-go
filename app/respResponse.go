package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func constructResponse(commands []string) string {
	switch commands[0] {
	case "ping":
		return pingResponse()
	case "echo":
		return echoResponse(commands)
	case "set":
		return setResponse(commands)
	case "get":
		return getResponse(commands)
	}
	return "-ERROR"
}

func pingResponse() string {
	return "+PONG\r\n"
}

func echoResponse(commands []string) string {
	arg := ""
	if len(commands) >= 1 {
		arg = commands[1]
	}
	return fmt.Sprintf("+%s\r\n", arg)
}

func setResponse(commands []string) string {
	// Set up the record
	record := Record{}

	// parse the commands (set [key] [value] ...args)
	key := commands[1]
	record.value = commands[2]

	args := commands[3:]
	for i, command := range args { // Handle the args
		switch command {
		case "px": // set a timeout on the record
			t, err := strconv.ParseInt(args[i+1], 0, 0)
			if err != nil {
				fmt.Println("Error parsing resp: SET: Timeout invalid.")
				os.Exit(0)
			}

			timeout := time.Duration(t) * time.Millisecond
			record.expiresAt = time.Now().Add(time.Duration(timeout))
			record.timeBomb = true
		}
	}

	keyValueStore[key] = record

	return "+OK\r\n"
}

func getResponse(commands []string) string {
	val, exists := keyValueStore[commands[1]]
	if !exists {
		return ""
	}
	if val.timeBomb && val.expiresAt.Compare(time.Now()) == -1 { // expired
		fmt.Println("Current Time:", time.Now())
		fmt.Println("Expiry Time:", val.expiresAt)
		return "$-1\r\n"
	}
	length := len(val.value)
	value := val.value
	return fmt.Sprintf("$%d\r\n%s\r\n", length, value)
}
