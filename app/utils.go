package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Logs the error and message and exits with os.Exit(0).
func logAndExit(message string, err error) {
	fmt.Println(message, ":", err)
	os.Exit(0)
}

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
	// Generate a full entry ID if newEntryID is "*"
	if newEntryID == "*" {
		// ID generation logic goes here if needed
		timestamp := time.Now().UTC().UnixMilli()
		newEntryID = fmt.Sprintf("%d-*", timestamp) // yes this is a cheap trick. proud of it tho
	}

	// Split the provided entry ID into timestamp and sequence parts
	newIdParts := strings.Split(newEntryID, "-")
	if len(newIdParts) < 2 {
		return "", fmt.Errorf("invalid entry ID format: %s", newEntryID)
	}

	timestamp := newIdParts[0]
	sequence := newIdParts[1]

	// If only the sequence number is "*", generate it based on the timestamp
	if timestamp != "*" && sequence == "*" {
		for existingID := range stream.entries {
			existingIdParts := strings.Split(existingID, "-")
			existingTimestamp := existingIdParts[0]

			if existingTimestamp == timestamp {
				seqNum, _ := strconv.Atoi(existingIdParts[1])
				return fmt.Sprintf("%s-%d", timestamp, seqNum+1), nil
			}
		}

		// Handle special case for "0" timestamp
		if timestamp == "0" {
			return fmt.Sprintf("%s-%d", timestamp, 1), nil
		}
		return fmt.Sprintf("%s-%d", timestamp, 0), nil
	}

	// Validate the full entry ID
	isValid, err := validateStreamEntryID(newEntryID)
	if !isValid || err != nil {
		return "", err
	}

	// Return the valid entry ID
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
