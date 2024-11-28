package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// registers the replica that is now connected to (this) master server.
func registerReplica(replicaConn net.Conn) {
	CONFIG.replicas = append(
		CONFIG.replicas,
		Replica{
			conn:   replicaConn,
			offset: 0,
		},
	)
}

func handleStreamEntryID(stream RedisStream, newEntryID string) (string, error) {
	// handle the entry ID generation/validation
	if newEntryID == "*" { // generate the entire entryID

	}

	splitEntryId := strings.Split(newEntryID, "-")
	if len(splitEntryId) < 2 {
		return "", fmt.Errorf("yeah idk how you end up here")
	}

	entryIdTimestamp := splitEntryId[0]
	entryIdSeqNo := splitEntryId[1]
	if entryIdTimestamp != "*" && entryIdSeqNo == "*" { // only autogenereate the entryID sequence number
		// need to check if there is an entry with that entryID timestamp
		for streamEntryID := range stream.entries {
			splitStreamEntryID := strings.Split(streamEntryID, "-")
			t := splitStreamEntryID[0]

			if t == entryIdTimestamp {
				s, _ := strconv.Atoi(entryIdSeqNo)
				return fmt.Sprintf("%s-%d", entryIdTimestamp, s+1), nil
			}
		}

	}

	// validate the entry ID
	valid, err := validateStreamEntryID(newEntryID)
	if !valid || err != nil {
		return "", err
	}

	return newEntryID, nil
}

// validates the stream entry ID to be correct. error returned from this should be the reply on XADD if invalid.
func validateStreamEntryID(entryID string) (bool, error) {
	splitEntryID := strings.Split(entryID, "-")
	if len(splitEntryID) != 2 {
		return false, fmt.Errorf("invalid entry id given for stream")
	}

	entryTimestamp, _ := strconv.Atoi(splitEntryID[0])
	entrySeqNum, _ := strconv.Atoi(splitEntryID[1])

	if entryTimestamp == 0 && entrySeqNum < 1 {
		return false, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	if entryID <= RDB.streamStore.lastStreamEntryID {
		return false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return true, nil
}
