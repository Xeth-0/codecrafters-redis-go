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
		return onEcho(commands)
	case "set":
		return onSet(commands)
	case "get":
		return onGet(commands)
	case "config":
		if len(commands) >= 2 && commands[1] == "get" {
			return onConfig(commands)
		}
	}
	return "-ERROR"
}

func pingResponse() string {
	return respEncodeString("PONG")
}

func onConfig(commands []string) string {
	response := ""
	args := commands[2:]

	count := 0
	for _, arg := range args {
		switch arg {
		case "dir":
			response += respEncodeBulkString("dir")
			response += respEncodeBulkString(RDB.config.dir)
			count += 2
		case "dbfilename":
			response += respEncodeBulkString("dbfilename")
			response += respEncodeBulkString(RDB.config.dbFileName)
			count += 2
		}
	}
	return fmt.Sprintf("*%d\r\n", count) + response
}

func onEcho(commands []string) string {
	arg := ""
	if len(commands) >= 2 {
		arg = commands[1]
	} else {
		return arg
	}

	return respEncodeString(arg)
}

func onSet(commands []string) string {
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

	return respEncodeString("OK")
}

func onGet(commands []string) string {
	val, exists := keyValueStore[commands[1]]
	if !exists {
		return ""
	}
	if val.timeBomb && val.expiresAt.Compare(time.Now()) == -1 { // expired
		return "$-1\r\n"
	}

	return respEncodeBulkString(val.value)
}
