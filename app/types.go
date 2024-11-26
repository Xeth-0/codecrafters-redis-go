package main

import (
	"net"
	"time"
)

// RESP Type signifier for comparison
type RESPType byte

// RESP Request Types
var RESPTypes = struct {
	Integer RESPType // Integer requests. Start with ':'
	String  RESPType // String requests. Start with '+'
	Bulk    RESPType // Bulk String requests. Start with '$'
	Array   RESPType // Array requests. Can contain other arrays as well. Start with '*'
	Error   RESPType // Error requests. Start with '-'
}{
	Integer: ':',
	String:  '+',
	Bulk:    '$',
	Array:   '*',
	Error:   '-',
}

// RESP request data store
type RESPData struct {
	String   string   // for string RESP requests/subrequests
	Int      int      // for int ...
	Array    []RESP   // for array.... Will hold nested RESP requests to handle arrays in arrays, or maps in arrays
	RespType RESPType // identifies the type
}

// Struct to hold the RESP parsed request
type RESP struct {
	respType RESPType // Type of data in the resp (String/Int/Bulk/Array/Error)
	RawBytes []byte   // The raw data read from the client (array of bytes)
	Length   int      // Length of the raw resp
	respData RESPData // The meat of the resp request (String/Int/Bulk/Array/Error)
}

// Value stored in the in-memory key-value store.
type Record struct {
	value     string    // string value the key will correspond to
	expires   bool      // will expire or not
	expiresAt time.Time // time at which the value will be inaccessible. (Using a passive delete)
}

// Config values for the RDB used.
type RDBConfig struct {
	dir        string // directory of the rdb file
	dbFileName string // filename for the .rdb file
}

// RDB in-mem representation.
type redisRDB struct {
	config        RDBConfig
	databaseStore map[string]Record // stores the key-value pairs
	// auxFields map[string]string // Auxiliary fields (string just because)
}

type RedisConfig struct {
	isSlave          bool
	masterHost       string // info for the master if isSlave
	masterPort       string
	masterReplID     string // info to pass to slaves if not isSlave
	masterReplOffset int

	// info map[string]map[string]any // nothing for now
	replicas []Replica

	rdbDir        string // rdb config options
	rdbDbFileName string

	port int // port to bind the server to
}

type Replica struct {
	conn   net.Conn
	offset int
	// some way to track ACKs.
}
