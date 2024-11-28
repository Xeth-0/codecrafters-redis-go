package main

import (
	"fmt"
	"net"
	"strings"
	"strconv"
)
func registerReplica(replicaConn net.Conn) {
	CONFIG.replicas = append(
		CONFIG.replicas,
		Replica{
			conn:   replicaConn,
			offset: 0,
		},
	)
}

func validateStreamEntryID(entryID string) (bool, error) {
	splitEntryID := strings.Split(entryID, "-")
	if len(splitEntryID) != 2{
		return false, fmt.Errorf("invalid entry id given for stream")
	}

	entryTimestamp, _ := strconv.Atoi(splitEntryID[0])
	entrySeqNum, _ := strconv.Atoi(splitEntryID[1])
	
	if entryTimestamp == 0 && entrySeqNum < 1 {
		return false, fmt.Errorf("invalid entry id given for stream: below the minimum entry id supported")
	}

	// check if the timestamp isn't before any others
	if entryID <= RDB.streamStore.lastStreamEntryID {
		return false, fmt.Errorf("invalid entry id given for stream: ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return true, nil
}