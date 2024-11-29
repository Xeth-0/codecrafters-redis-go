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

// RESP request data store. Use the RespType field to determine which field holds the data.
type RESPData struct {
	String   string   // holds the data for string RESP requests/subrequests.
	Int      int      // holds the data for int RESP requests/subrequests.
	Array    []RESP   // holds the data for array.... Will hold nested RESP requests to handle arrays in arrays, or maps in arrays.
	RespType RESPType // identifies the type of data stored in the structure.
}

// Struct to hold the RESP parsed request.
type RESP struct {
	respType RESPType // Type of data in the resp (String/Int/Bulk/Array/Error)
	RawBytes []byte   // The raw data read from the client (array of bytes)
	Length   int      // Length of the raw resp
	respData RESPData // The meat of the resp request (String/Int/Bulk/Array/Error)
}

// Value stored in the in-memory key-value store.
type RedisRecord struct {
	value     string    // string value the key will correspond to
	expires   bool      // will expire or not
	expiresAt time.Time // time at which the value will be inaccessible. (Using a passive delete)
}

// Maybe this could've been the same as redisRDB, with just the valuetype as an added property. Will change later if there's no benefit to this.
// This also should be a radix-trie. I uhhh, i can't do that yet. For now, it's a map, within a map. with a bunch of hacks for the functionalities.

// Config values for the RDB used.
type RDBConfig struct {
	dir        string // directory of the rdb file
	dbFileName string // filename for the .rdb file
}

// Represents a single entry in a stream. Stores all the key-values for that particular stream.
type StreamEntry struct {
	id     string            // id for the individual stream
	fields map[string]string // key-value pairs for the stream
	keys   []string          // stores the keys for the values, in order recieved.
}

// Single redis stream.
type RedisStream struct {
	entries    map[string]*StreamEntry // Map of entries by ID
	entryOrder []string                // Maintain the order of entry.
	blockCh    chan string
}

type RedisStreamStore struct {
	streams           map[string]RedisStream
	lastStreamEntryID string // ID of the last inerted entry. This will let us check quickly for ops that require the last entry.
}

type RedisKeyValueStore struct {
	db map[string]RedisRecord
}

// RDB in-mem representation.
type RedisRDB struct {
	config        RDBConfig
	keyValueStore RedisKeyValueStore // stores the key-value pairs (get/set)
	streamStore   RedisStreamStore   // stores redis-streams, key is the stream name(key) (xadd/xread)
	// auxFields map[string]string // Auxiliary fields (string just because)
}

// Stores configuration options for the current Redis Server.
type RedisConfig struct {
	isSlave          bool   // Is this server a master or slave
	masterHost       string // info for the master if isSlave
	masterPort       string // port the master is running on (empty string if master server)
	masterReplID     string // replication id of the master (empty string if slave)
	masterReplOffset int    // replciation offset of the master

	replicas []Replica // Stores the replicas connected to this server (if master)

	rdbDir        string // rdb config options
	rdbDbFileName string // filename for the rdb to load
	port          int    // port to bind the server to

	transactions Transactions
}

type Transactions struct {
	transactionsCalled int
	commandQueue       [][][]string
}

// Stores info for a single replica server.
type Replica struct {
	conn   net.Conn
	offset int
}
