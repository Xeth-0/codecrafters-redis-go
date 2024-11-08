package main

import "time"

// RESP Type signifier for comparison
type RESPType byte

// RESP Types
var RESPTypes = struct {
	Integer RESPType
	String  RESPType
	Bulk    RESPType
	Array   RESPType
	Error   RESPType
}{
	Integer: ':',
	String:  '+',
	Bulk:    '$',
	Array:   '*',
	Error:   '-',
}

// RESP request data store
type RESPData struct {
	String string // for string RESP requests/subrequests
	Int    int    // for int ...
	Array  []RESP // for array.... Will hold nested RESP requests to handle arrays in arrays, or maps in arrays
}

// Struct to hold the RESP parsed request
type RESP struct {
	respType RESPType // Type of data in the resp (String/Int/Bulk/Array/Error)
	RawBytes []byte   // The raw data read from the client (array of bytes)
	Data     []byte   // Request excluding CRLF and Type // TODO: REMOVE THIS FIELD
	Length   int      // Length of the raw resp
	respData RESPData // The meat of the resp request (String/Int/Bulk/Array/Error)
}

// Value stored in the in-memory key-value store.
type Record struct {
	value string
	expiresAt time.Time
}
