package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func executeResp(commands []string, conn net.Conn) (responses []string, err error) {
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
	case "replconf":
		return onReplConf(commands)
	case "psync":
		CONFIG.replicas = append(CONFIG.replicas, conn) // Save the connection as a replica for propagation.
		return onPSync()
	}
	return nil, fmt.Errorf("error parsing request")
}

func onPSync() ([]string, error) {
	// args := commands[1:]
	responses := make([]string, 0, 3)

	// Need to send 2 responses. First a FULL RESYNC response. Then, an encoded version of the rdb store.
	response := respEncodeString(fmt.Sprintf("FULLRESYNC %s %d", CONFIG.masterReplID, CONFIG.masterReplOffset))
	responses = append(responses, response)

	// Unlike regular bulk strings, the encoded rdb we have to send to the server doesnt have the final /r/n
	encodedRDB := string(encodeRDB(RDB))
	response = respEncodeBulkString(encodedRDB)

	// remove that final /r/n at the end of the bulk encoded string
	response = response[:len(response)-2]
	responses = append(responses, response)
	return responses, nil
}

func onReplConf(commands []string) ([]string, error) {
	args := commands[1:]
	if args[0] == "getack" {
		if args[1] == "*" {
			response := []string{"REPLCONF", "ACK", "0"}
			return []string{respEncodeStringArray(response)}, nil
		}
	} 

	response := respEncodeString("OK")
	responses := []string{response}
	return responses, nil
}

func onInfo(commands []string) ([]string, error) {
	args := commands[1:]
	responses := make([]string, 0, 3)

	for _, arg := range args {
		switch arg {
		case "replication":
			if CONFIG.isSlave {
				response := respEncodeBulkString("role:slave")
				responses = append(responses, response)
			}

			rawResponse := fmt.Sprintf("role:master\r\nmaster_repl_offset:%d\r\nmaster_replid:%s", CONFIG.masterReplOffset, CONFIG.masterReplID)
			response := respEncodeBulkString(rawResponse)
			responses = append(responses, response)
			return responses, nil
		}
	}
	return responses, fmt.Errorf("error handling request: INFO - replication flag not provided (all this can handle at the moment)")
}

func onCommand(commands []string) ([]string, error) {
	args := commands[1:]

	if args[0] == "docs" { // default request when initiating a redis-cli connection
		return onPing()
	}

	return onPing() // just because. // TODO: Fix later
}

func onPing() ([]string, error) {
	response := respEncodeString("PONG")
	responses := []string{response}

	return responses, nil
}

func onKeys(commands []string) ([]string, error) {
	args := commands[1:]
	responses := make([]string, 0, 3)

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

		response := respEncodeStringArray(keys)
		responses = append(responses, response)

		return responses, nil
	} else {
		// TODO: If a patern is provided. needs regex matching, will do later.
	}
	return responses, fmt.Errorf("error handling request: KEYS - '*' not provided")
}

func onConfigGet(commands []string) ([]string, error) {
	args := commands[2:]

	response := ""
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
	response = fmt.Sprintf("*%d\r\n", count) + response
	responses := []string{response}
	return responses, nil
}

func onEcho(commands []string) ([]string, error) {
	arg := ""
	responses := make([]string, 0, 1)
	if len(commands) >= 2 {
		arg = commands[1]
	} else {
		responses = append(responses, arg)
		return responses, nil
	}
	response := respEncodeString(arg)
	responses = append(responses, response)
	return responses, nil
}

func onSet(commands []string) ([]string, error) {
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
	response := respEncodeString("OK")
	responses := []string{response}
	return responses, nil
}

func onGet(commands []string) ([]string, error) {
	responses := make([]string, 0, 1)
	val, exists := RDB.databaseStore[commands[1]]
	if !exists {
		return responses, fmt.Errorf("error handling request: GET - value not found")
	}
	if val.expires && val.expiresAt.Compare(time.Now()) == -1 {
		// expired
		responses = append(responses, "$-1\r\n")
		return responses, nil
	}

	response := respEncodeBulkString(val.value)
	responses = append(responses, response)
	return responses, nil
}
