package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func handleResponse(commands []string, conn net.Conn) (string, error) {
	switch commands[0] {

	case "ping":
		return onPing(conn)
	case "echo":
		return onEcho(commands, conn)
	case "set":
		return onSet(commands, conn)
	case "get":
		return onGet(commands, conn)
	case "config":
		if len(commands) >= 2 && commands[1] == "get" {
			return onConfigGet(commands, conn)
		}
	case "keys":
		return onKeys(commands, conn)
	case "command":
		return onCommand(commands, conn)
	case "info":
		return onInfo(commands, conn)
	case "replconf":
		return onReplConf(conn)

	case "psync":
		return onPSync(conn)
	}
	return "", fmt.Errorf("error parsing request")
}

func onPSync(conn net.Conn) (string, error) {
	// args := commands[1:]
	// Need to send 2 responses. First a FULLRESYNC response. Then, an encoded version of the rdb store.
	response := respEncodeString(fmt.Sprintf("FULLRESYNC %s %d", CONFIG.masterReplID, CONFIG.masterReplOffset))
	conn.Write([]byte(response))

	// Unlike regular bulk strings, the encoded rdb we have to send to the server doesnt have the final /r/n
	encodedRDB := string(encodeRDB(RDB))
	response = respEncodeBulkString(encodedRDB)

	// remove that final /r/n at the end of the bulk encoded string
	response = response[:len(response)-2]
	conn.Write([]byte(response))
	return "", nil
}

func onReplConf(conn net.Conn) (string, error) {
	response := respEncodeString("OK")
	conn.Write([]byte(response))
	return response, nil
}

func onInfo(commands []string, conn net.Conn) (string, error) {
	args := commands[1:]

	for _, arg := range args {
		switch arg {
		case "replication":
			if CONFIG.isSlave {
				response := respEncodeBulkString("role:slave")
				conn.Write([]byte(response))
			}

			rawResponse := fmt.Sprintf("role:master\r\nmaster_repl_offset:%d\r\nmaster_replid:%s", CONFIG.masterReplOffset, CONFIG.masterReplID)
			response := respEncodeBulkString(rawResponse)
			conn.Write([]byte(response))
			return response, nil
		}
	}
	return "", fmt.Errorf("error handling request: INFO - replication flag not provided (all this can handle at the moment)")
}

func onCommand(commands []string, conn net.Conn) (string, error) {
	args := commands[1:]

	if args[0] == "docs" { // default request when initiating a redis-cli connection
		return onPing(conn)
	}

	return onPing(conn) // just because. // TODO: Fix later
}

func onPing(conn net.Conn) (string, error) {
	response := respEncodeString("PONG")
	conn.Write([]byte(response))
	return response, nil
}

func onKeys(commands []string, conn net.Conn) (string, error) {
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

		response := respEncodeStringArray(keys)
		conn.Write([]byte(response))
		return response, nil
	} else {
		// TODO
		// If a patern is provided. his is regex matching, will do later.
	}
	return "", fmt.Errorf("error handling request: KEYS - '*' not provided")
}

func onConfigGet(commands []string, conn net.Conn) (string, error) {
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
	response = fmt.Sprintf("*%d\r\n", count) + response
	conn.Write([]byte(response))
	return response, nil
}

func onEcho(commands []string, conn net.Conn) (string, error) {
	arg := ""
	if len(commands) >= 2 {
		arg = commands[1]
	} else {
		conn.Write([]byte(arg))
		return arg, nil
	}

	response := respEncodeString(arg)
	conn.Write([]byte(response))
	return response, nil
}

func onSet(commands []string, conn net.Conn) (string, error) {
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
	conn.Write([]byte(response))
	return response, nil
}

func onGet(commands []string, conn net.Conn) (string, error) {
	val, exists := RDB.databaseStore[commands[1]]
	if !exists {
		return "", fmt.Errorf("error handling request: GET - value not found")
	}
	if val.expires && val.expiresAt.Compare(time.Now()) == -1 {
		// expired
		conn.Write([]byte("$-1\r\n"))
		return "$-1\r\n", nil
	}

	response := respEncodeBulkString(val.value)
	conn.Write([]byte(response))
	return response, nil
}
