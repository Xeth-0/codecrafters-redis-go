package main

import (
	"fmt"
	"net"
	"strings"
	"strconv"
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

// validates the stream entry ID to be correct. error returned from this should be the reply on XADD if invalid.
func validateStreamEntryID(entryID string) (bool, error) {
	splitEntryID := strings.Split(entryID, "-")
	if len(splitEntryID) != 2{
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