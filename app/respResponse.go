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
		return onPing()
	case "echo":
		return onEcho(commands)
	case "set":
		return onSet(commands)
	case "get":
		return onGet(commands)
	case "config":
		if len(commands) >= 2 && commands[1] == "get" {
			return onConfigGet(commands)
		}
	case "keys":
		return onKeys(commands)
	case "command":
		return onCommand(commands)
	case "info":
		return onInfo(commands)

	}
	return "-ERROR"
}

func onInfo(commands []string) string {
	args := commands[1:]

	for _, arg := range args {
		switch arg {
		case "replication":
			if CONFIG.isSlave {
				return respEncodeBulkString("role:slave")
			}

			resp := fmt.Sprintf("role:master\r\nmaster_repl_offset:%d\r\nmaster_replid:%s", CONFIG.masterReplOffset, CONFIG.masterReplID)
			return respEncodeBulkString(resp)
		}
	}
	return ""
}

func onCommand(commands []string) string {
	args := commands[1:]

	if args[0] == "docs" { // default request when initiating a redis-cli connection
		return onPing()
	}

	return onPing() // just because. // TODO: Fix later
}

func onPing() string {
	return respEncodeString("PONG")
}

func onKeys(commands []string) string {
	args := commands[1:]

	if len(args) < 1 {
		fmt.Println("Not enough args in command")
		os.Exit(0)
	}

	if args[0] == "*" {
		// return all keys
		keys := make([]string, 0, len(RDB.databaseStore))
		for k := range RDB.databaseStore {
			keys = append(keys, k)
		}

		return respEncodeStringArray(keys)
	}

	return ""
}

func onConfigGet(commands []string) string {
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
				fmt.Println("Error parsing resp: SET Command: Timeout (ms) is invalid.")
				os.Exit(0)
			}

			timeout := time.Duration(t) * time.Millisecond
			record.expiresAt = time.Now().Add(time.Duration(timeout))
			record.expires = true

		case "ex": // set timeout in seconds
			t, err := strconv.ParseInt(args[i+1], 0, 0)
			if err != nil {
				fmt.Println("error parsing resp: SET Command: Timeout (s) is invalid")
			}

			timeout := time.Duration(t)
			record.expiresAt = time.Now().Add(time.Duration(timeout))
			record.expires = true
		}
	}

	RDB.databaseStore[key] = record
	return respEncodeString("OK")
}

func onGet(commands []string) string {
	val, exists := RDB.databaseStore[commands[1]]
	if !exists {
		return ""
	}
	if val.expires && val.expiresAt.Compare(time.Now()) == -1 { // expired
		return "$-1\r\n"
	}

	return respEncodeBulkString(val.value)
}
