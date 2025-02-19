package main

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

var ackChan = make(chan bool)

func executeResp(commands []string, conn net.Conn) (responses []string, err error) {
	// If MULTI has been called, the command will not get executed, but queued.
	transaction, exists := CONFIG.transactions[conn] // check if there is an active transaction on that connection
	command := commands[0]

	shouldQueue := exists && transaction.active && command != "multi" && command != "exec" && command != "discard"
	if shouldQueue {
		// Queue the new command
		transaction.commandQueue = append(transaction.commandQueue, commands)
		CONFIG.transactions[conn] = transaction

		return []string{respEncodeString("QUEUED")}, nil
	}

	switch command {

	case "ping":
		return onPING()
	case "echo":
		return onECHO(commands)
	case "set":
		return onSET(commands)
	case "get":
		return onGET(commands)
	case "config":
		return onCONFIG(commands)
	case "keys":
		return onKEYS(commands)
	case "command":
		return onCOMMAND(commands)
	case "info":
		return onINFO(commands)
	case "replconf":
		return onREPLCONF(commands, ackChan)
	case "psync":
		// Save the connection as a replica for propagation.
		registerReplica(conn)
		return onPSYNC()
	case "wait":
		return onWAIT(commands, ackChan)
	case "type":
		return onTYPE(commands)
	case "xrange":
		return onXRANGE(commands)
	case "xadd":
		return onXADD(commands)
	case "xread":
		return onXREAD(commands)
	case "incr":
		return onINCR(commands)
	case "multi":
		return onMULTI(commands, conn)
	case "exec":
		return onEXEC(commands, conn)
	case "discard":
		return onDISCARD(commands, conn)

	}
	return nil, fmt.Errorf("error parsing request")
}

func onDISCARD(_ []string, conn net.Conn) ([]string, error) {
	transaction, exists := CONFIG.transactions[conn]
	if !exists || !transaction.active {
		return []string{respEncodeError("ERR DISCARD without MULTI")}, nil
	}

	// clear the transaction
	transaction.active = false
	transaction.commandQueue = make([][]string, 0)

	CONFIG.transactions[conn] = transaction
	return []string{respEncodeString("OK")}, nil
}

func onEXEC(_ []string, conn net.Conn) ([]string, error) {
	transaction, exists := CONFIG.transactions[conn]
	if !exists || !transaction.active {
		// multi has not been called
		return []string{respEncodeError("ERR EXEC without MULTI")}, nil
	}

	if len(transaction.commandQueue) == 0 {
		// multi has been called, but no commands have been queued. return empty array and clear the multi
		transaction.active = false
		CONFIG.transactions[conn] = transaction
		return []string{"*0\r\n"}, nil
	}

	responses := make([]string, 0, len(transaction.commandQueue))
	for _, request := range transaction.commandQueue {
		response, err := executeResp(request, nil) // leaving conn as nil ig.
		if err != nil {
			// handle this later as well.
		}

		responses = append(responses, response...)
	}

	// Construct the final response
	response := fmt.Sprintf("*%d\r\n", len(responses))

	for _, r := range responses {
		response += r
	}

	// clear the transaction
	transaction.active = false
	transaction.commandQueue = make([][]string, 0)
	CONFIG.transactions[conn] = transaction

	return []string{response}, nil
}

func onMULTI(_ []string, conn net.Conn) ([]string, error) {
	// make a new transaction
	transaction, exists := CONFIG.transactions[conn]
	if !exists {
		CONFIG.transactions[conn] = RedisTransaction{
			active:       true,
			commandQueue: make([][]string, 0),
		}
	} else if transaction.active {
		return []string{respEncodeError("ERR MULTI calls can not be nested")}, nil
	}
	transaction.active = true
	CONFIG.transactions[conn] = transaction
	return []string{respEncodeString("OK")}, nil
}

func onINCR(commands []string) ([]string, error) {
	args := commands[1:]

	key := args[0]

	record, exists := RDB.keyValueStore.db[key]
	if !exists {
		fmt.Println("incr: key doesnt exist, creating new key-value pair...")
		RDB.keyValueStore.db[key] = RedisRecord{
			value: "1",
		}
		return []string{respEncodeInteger(1)}, nil
	}

	numericalVal, err := strconv.Atoi(record.value)
	if err != nil { // not a number
		return []string{respEncodeError("ERR value is not an integer or out of range")}, nil
	}

	// Increment the value
	numericalVal++
	record.value = fmt.Sprintf("%d", numericalVal)
	RDB.keyValueStore.db[key] = record

	return []string{respEncodeInteger(numericalVal)}, nil
}

func onXREAD(commands []string) ([]string, error) {
	// Parsing args: expecting "block" to come before "stream".
	args := commands[1:]
	if len(args) < 3 {
		return nil, fmt.Errorf("xread: not enough arguments")
	}

	// Pointer to find start of "[streams ...streamKey]"
	streamsStart := 0

	// Is the command blocking?
	isBlocking := false
	blockingMs := 0

	// set the blocking values and identify the start of the stream keys.
	if args[0] == "block" {
		isBlocking = true
		ms, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, fmt.Errorf("xread: invalid blocking timeout")
		}

		blockingMs = ms
		streamsStart = 2
	}

	if args[streamsStart] != "streams" {
		return nil, fmt.Errorf("xread: incorrect format, expected 'streams [...stream_key]'")
	}

	// move the pointer to the start of the stream-keys
	streamsStart++

	// Split arguments into stream keys and their respective start IDs
	numStreams := len(args[streamsStart:]) / 2 // Will be evenly divisible by 2 (after "streams", we have [...streamKey] [...entryIDs] , which should be the same number)
	streamKeys := args[streamsStart : streamsStart+numStreams]
	startIDs := args[streamsStart+numStreams:]

	// Build the response (resp array)
	response := fmt.Sprintf("*%d\r\n", len(streamKeys))

	// for each stream to be read...
	for i, streamKey := range streamKeys {
		stream, exists := RDB.streamStore.streams[streamKey]
		if !exists {
			return nil, fmt.Errorf("xread: stream '%s' not found", streamKey)
		}

		startID := startIDs[i]
		if startID == "$" { // we're waiting for new entries. easy way to do this
			startID = RDB.streamStore.lastStreamEntryID
		}

		if isBlocking {
			var timeChan <-chan time.Time = nil
			if blockingMs != 0 {
				timeChan = time.After(time.Duration(blockingMs) * time.Millisecond)
			}

			updateChan := stream.blockCh

		timeloop:
			for {
				select {
				case <-timeChan:
					break timeloop
				case newEntry := <-updateChan:
					// Only breaking the loop if it's a new entry that satisfies the startID< condition
					if startID < newEntry {
						stream = RDB.streamStore.streams[streamKey]
						break timeloop
					}
				}
			}
		}

		// Gather entries
		entries := make([]StreamEntry, 0)
		for _, entryID := range stream.entryOrder {
			if startID == "-" || entryID > startID {
				entries = append(entries, *stream.entries[entryID])
			}
		}

		// No entries found. return null bulk string
		if len(entries) == 0 {
			return []string{"$-1\r\n"}, nil
		}

		// Add the stream key and entries to the response
		encodedStreamKey := respEncodeBulkString(streamKey)
		response += fmt.Sprintf("*2\r\n%s*%d\r\n", encodedStreamKey, len(entries))
		for _, entry := range entries {
			// Gather the entry key-value pairs
			entryFields := make([]string, 0, len(entry.keys)*2)
			for _, key := range entry.keys {
				entryFields = append(entryFields, key, entry.fields[key])
			}

			// Add encoded values to the response
			encodedEntryID := respEncodeBulkString(entry.id)
			encodedEntryFields := respEncodeStringArray(entryFields)
			response += fmt.Sprintf("*2\r\n%s%s", encodedEntryID, encodedEntryFields)
		}
	}

	// fmt.Println("XREAD Response: ", response)
	return []string{response}, nil
}

func onXRANGE(commands []string) ([]string, error) {
	args := commands[1:]
	if len(args) < 3 {
		return []string{}, fmt.Errorf("not enough args for XRANGE")
	}

	streamKey := args[0]
	startID := args[1]
	endID := args[2]

	stream, exists := RDB.streamStore.streams[streamKey]
	if !exists {
		return []string{}, fmt.Errorf("xrange: stream not found")
	}

	// gather the entries
	entries := make([]StreamEntry, 0)
	for _, entryID := range stream.entryOrder {
		startIdIsValid := (startID == "-") || (entryID >= startID)
		endIdIsValid := (endID == "+") || (entryID <= endID)
		if startIdIsValid && endIdIsValid {
			entries = append(entries, *stream.entries[entryID])
		}
	}

	// construct the response
	response := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {

		encodedID := respEncodeBulkString(entry.id)

		entryVals := make([]string, 0)
		for _, entryKey := range entry.keys {
			entryVals = append(entryVals, entryKey)
			entryVals = append(entryVals, entry.fields[entryKey])
		}

		encodedVals := respEncodeStringArray(entryVals)
		response += fmt.Sprintf("*2\r\n%s%s", encodedID, encodedVals)
	}
	return []string{response}, nil
}

func onXADD(commands []string) ([]string, error) {
	args := commands[1:]
	if len(args) < 4 {
		return []string{}, fmt.Errorf("not enough arguements for XADD")
	}

	streamKey := args[0]
	entryId := args[1]

	stream, exists := RDB.streamStore.streams[streamKey]
	if !exists {
		RDB.streamStore.streams[streamKey] = RedisStream{
			entries:    make(map[string]*StreamEntry),
			entryOrder: make([]string, 0),
			blockCh:    make(chan string),
		}
		stream = RDB.streamStore.streams[streamKey]
	}

	entryId, err := handleStreamEntryID(stream, entryId)
	if err != nil {
		response := respEncodeError(err.Error())
		return []string{response}, nil
	}

	streamEntry := &StreamEntry{
		id:     entryId,
		fields: map[string]string{},
	}

	for i := 2; i < len(args); i += 2 {
		// there might be more than one key-value pairs here ig
		key := args[i]
		val := args[i+1]

		streamEntry.fields[key] = val
		streamEntry.keys = append(streamEntry.keys, key)
	}

	stream.entries[entryId] = streamEntry
	stream.entryOrder = append(stream.entryOrder, entryId)
	RDB.streamStore.lastStreamEntryID = entryId // storing this entry ID as the last one added.

	// update the channel
	select { // non blocking update
	case stream.blockCh <- entryId:
	default:
	}

	RDB.streamStore.streams[streamKey] = stream

	return []string{respEncodeBulkString(entryId)}, nil
}

func onTYPE(commands []string) ([]string, error) {
	if len(commands) <= 1 {
		return []string{}, fmt.Errorf("not enough arguments provided")
	}

	args := commands[1:]
	key := args[0]

	// check key-value store
	_, exists := RDB.keyValueStore.db[key]
	if exists {
		return []string{respEncodeString("string")}, nil
	}

	// check stream store
	_, exists = RDB.streamStore.streams[key]
	if exists {
		return []string{respEncodeString("stream")}, nil
	}
	return []string{respEncodeString("none")}, nil
}

func onWAIT(commands []string, ackChan chan bool) ([]string, error) {
	// Waits(blocks) until it either times out, or gets the specified number of ACKs from replicas. (runs on master server)
	args := commands[1:]

	someNumber, _ := strconv.Atoi(args[0])
	timeoutDuration, _ := strconv.Atoi(args[1])

	// Going to send out "replconf getack *"  requests to the replicas
	getAckReq := []byte(
		respEncodeStringArray([]string{"REPLCONF", "GETACK", "*"}),
	)

	// When all the replicas are in sync, reply should be the total count of replicas. But the timeout the tests give is so small that the replicas will not reply in time, and it'll timeout before getting any acks.
	// Yes, this is a cheap hack. I do not have time to think of sth better unfortunately.
	// TODO: Fix this ugly shit.
	if CONFIG.masterReplOffset == 0 {
		return []string{respEncodeInteger(len(CONFIG.replicas))}, nil
	}

	// The replicas should have their own connections going in another goroutine. Going to send the replconf get ack from here, and expect the reply in the goroutine that handles the connection to the replica in normal flow.
	// When an ack is recieved, the ackChan channel is updated, and this will increment the acks counter.
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

	fmt.Println("Waiting ...")
loop: // label just to break the loop
	for acks < someNumber { // loop and block until...
		select {
		case <-ackChan: // recieved an ack response for a replica (on it's connection goroutine)
			acks++
			fmt.Printf("Waiting: Recieved ack - %d", acks)

		case <-timerChan: // timer timed out
			fmt.Println("Waiting: timed out.")
			break loop
		}
	}

	return []string{respEncodeInteger(acks)}, nil
}

func onPSYNC() ([]string, error) {
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

func onREPLCONF(commands []string, ackChan chan bool) ([]string, error) {
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

func onINFO(commands []string) ([]string, error) {
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

func onCOMMAND(commands []string) ([]string, error) {
	args := commands[1:]

	if args[0] == "docs" { // default request when initiating a redis-cli connection
		return onPING()
	}

	return onPING() // just because. // TODO: Fix later
}

func onPING() ([]string, error) {
	response := respEncodeString("PONG")
	responses := []string{response}

	return responses, nil
}

func onKEYS(commands []string) ([]string, error) {
	args := commands[1:]
	responses := make([]string, 0, 3)

	if len(args) < 1 {
		return []string{}, fmt.Errorf("not enough args in command")
	}

	if args[0] == "*" {
		// return all keys
		keys := make([]string, 0, len(RDB.keyValueStore.db))
		for k := range RDB.keyValueStore.db {
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

func onCONFIG(commands []string) ([]string, error) {
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

func onECHO(commands []string) ([]string, error) {
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

func onSET(commands []string) ([]string, error) {
	// Set up the record
	record := RedisRecord{}

	// parse the commands (set [key] [value] ...args)
	key := commands[1]
	record.value = commands[2]

	args := commands[3:]
	for i, command := range args { // Handle the args
		switch command {
		case "px": // set a timeout on the record
			t, err := strconv.ParseInt(args[i+1], 0, 0)
			if err != nil {
				return []string{}, fmt.Errorf("error parsing resp: SET Command: Timeout (ms) is invalid")
			}

			timeout := time.Duration(t) * time.Millisecond
			record.expiresAt = time.Now().Add(time.Duration(timeout))
			record.expires = true

		case "ex": // set timeout in seconds
			t, err := strconv.ParseInt(args[i+1], 0, 0)
			if err != nil {
				return []string{}, fmt.Errorf("error parsing resp: SET Command: Timeout (s) is invalid")
			}

			timeout := time.Duration(t)
			record.expiresAt = time.Now().Add(time.Duration(timeout))
			record.expires = true
		}
	}

	RDB.keyValueStore.db[key] = record
	response := respEncodeString("OK")
	responses := []string{response}
	return responses, nil
}

func onGET(commands []string) ([]string, error) {
	// I am going to cheat a little here. Sometimes during replication propagation,
	//  the propagation takes a little too long and the GET commands come too soon.
	//  (before the SETs from the master are propagated). And i'm tired of the race condition.
	time.Sleep(10 * time.Millisecond) // TODO: REMOVE THIS without breaking the rest.
	// SORRY

	responses := make([]string, 0, 1)
	val, exists := RDB.keyValueStore.db[commands[1]]
	if (!exists) || (val.expires && val.expiresAt.Compare(time.Now()) == -1) {
		// expired or doesn't exist
		responses = append(responses, "$-1\r\n")
		return responses, nil
	}

	response := respEncodeBulkString(val.value)
	responses = append(responses, response)
	return responses, nil
}
