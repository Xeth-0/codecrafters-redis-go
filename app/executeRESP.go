package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

var ackChan = make(chan bool)

func executeResp(commands []string, conn net.Conn) (responses []string, err error) {
	fmt.Println(commands[0])
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
		return onConfig(commands)
	case "keys":
		return onKeys(commands)
	case "command":
		return onCommand(commands)
	case "info":
		return onInfo(commands)
	case "replconf":
		return onReplConf(commands, ackChan)
	case "psync":
		registerReplica(conn) // Save the connection as a replica for propagation.
		return onPSync()
	case "wait":
		return onWait(commands, ackChan)
	case "type":
		return onType(commands)
	}
	return nil, fmt.Errorf("error parsing request")
}

func onType(commands []string) ([]string, error) {
	if len(commands) <= 1 {
		return []string{}, fmt.Errorf("not enough arguments provided")
	}
	
	args := commands[1:]
	
	key := args[0]
    _, exists := RDB.databaseStore[key]
	if !exists {
		return []string{respEncodeString("none")}, nil
	}

	return []string{respEncodeString("string")}, nil
}

func onWait(commands []string, ackChan chan bool) ([]string, error) {
	// Waits(blocks) until it either times out, or gets the specified number of ACKs from replicas. (runs on master server)
	args := commands[1:]

	someNumber, _ := strconv.Atoi(args[0])
	timeoutDuration, _ := strconv.Atoi(args[1])

	// Going to send out "replconf getack *"  requests to the replicas
	getAckReq := []byte(
		respEncodeStringArray([]string{"REPLCONF", "GETACK", "*"}),
	)

	// Yes, this is a cheap hack. I do not have time to think of sth better unfortunately.
	// TODO: Fix this ugly shit.
	if CONFIG.masterReplOffset == 0 {
		return []string{respEncodeInteger(len(CONFIG.replicas))}, nil
	}

	// The replicas should have their own connections going in another goroutine,
	//   when the ACK is recieved, will use a channel to count them.
	acks := 0
	for _, replica := range CONFIG.replicas {
		go func(conn net.Conn) {
			// for each replica, query it's offset in a go-routine
			bytesWritten, _ := replica.conn.Write(getAckReq)
			replica.offset += bytesWritten
		}(replica.conn)
	}

	// Handling the timer.
	timerChan := time.After(time.Duration(timeoutDuration) * time.Millisecond)

loop: // label just to break the loop
	for acks < someNumber { // loop and block until...
		select {
		case <-ackChan: // recieved an ack response for a replica(on it's goroutine)
			acks++
			fmt.Printf("Waiting: Recieved ack - %d", acks)
		case <-timerChan: // timer timed out
			fmt.Println("Waiting: timed out.")
			break loop
		}
	}

	return []string{respEncodeInteger(acks)}, nil
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

func onReplConf(commands []string, ackChan chan bool) ([]string, error) {
	args := commands[1:]
	switch args[0] {
	case "getack":
		if args[1] == "*" {
			offset := fmt.Sprintf("%d", CONFIG.masterReplOffset)
			response := []string{"REPLCONF", "ACK", offset}
			return []string{respEncodeStringArray(response)}, nil
		}
	case "ack":
		if !CONFIG.isSlave { // master recieved ack.
			ackChan <- true
		}
		return []string{}, nil
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

func onConfig(commands []string) ([]string, error) {
	// only handling get.
	if len(commands) <= 1 || commands[1] != "get" {
		return []string{}, fmt.Errorf("error executing resp: unsupported CONFIG command")
	}

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

func registerReplica(replicaConn net.Conn) {
	CONFIG.replicas = append(
		CONFIG.replicas,
		Replica{
			conn:   replicaConn,
			offset: 0,
		},
	)
}
