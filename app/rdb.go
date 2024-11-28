package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// opCodes for section delimiters/identifiers in the .rdb file to be read.
//  Each part after the initial header is introduced by a special op code.

//	   below are the binary versions of the hex encoded opcodes from
//		  https://rdb.fnordig.de/file_format.html#op-codes

const ( // Descriptions are also from the site, here for the convinience of not looking it up.
	opCodeAux          byte = 0xFA // Auxiliary Fields. Arbitrary key-value settings.
	opCodeResizeDB     byte = 0xFB // Hash table sizes for the main keyspace and expires.
	opCodeExpireTimeMs byte = 0xFC // Expire time in milliseconds.
	opCodeExpireTime   byte = 0xFD // Expire time in seconds.
	opCodeSelectDB     byte = 0xFE // DB Selector.
	opCodeEOF          byte = 0xFF // End of the RDB file.
)

const ( // String encoding identifier
	stringEncoding_string byte = 0x0D // Anything that has a first byte less than this is a string. (Max length for strings is 13)
	stringEncoding_int8   byte = 0xC0 // Regular int8
	stringEncoding_int16  byte = 0xC1 // Regular int16
	stringEncoding_int32  byte = 0xC2 // Little-Endian encoded int32
	stringEncoding_int64  byte = 0xC3 // Little-Endian encoded int64
)

const ( // Size encoding identifier
	sizeEncoding_6bits             byte = 0b00 // size is remaining 6 bits of this byte
	sizeEncoding_14bits            byte = 0b01 // size is next 14 bits(this plus next entire byte) in big-endian
	sizeEncoding_idkwhattocallthis byte = 0b10 // ignore first 6(rest of this byte), size is the 4 BYTES after in big-endian
	sizeEncoding_stringEncoding    byte = 0b11 // remaining 6 bits are string encoded
)

// Loads, reads and returns a struct containing the information from the
// .rdb file from the directory and filename provided.
func setupRDB(dir string, dbFileName string) RedisRDB {
	rdbConfig := RDBConfig{
		dir:        dir,
		dbFileName: dbFileName,
	}
	rdb := RedisRDB{
		config: rdbConfig,
	}
	rdb.keyValueStore = RedisKeyValueStore{db: make(map[string]RedisRecord)}
	rdb.streamStore = RedisStreamStore{streams: make(map[string]RedisStream)}

	// need to load in the rdb specified by the dirname and dir.
	data, exists := loadRDBFromFile(dir, dbFileName)
	if exists {
		// load the data from the rdb file.
		rdb = parseRDB(data, rdb)
	}

	return rdb
}

// parses the rdb data in bits and extracts useful information into the redisRDB struct.
func parseRDB(data []byte, rdb RedisRDB) RedisRDB {
	if len(data) < 9 {
		return rdb
	}

	// and now, for the looooooooong process of loading the bitch
	index := 0

	// skip the header (Magic String + Version Number => "REDIS0012")
	index += 9

	rdb, indexOffset := _parseRDB_MetaData(data[index:], rdb)
	index += indexOffset

	rdb, indexOffset = _parseRDB_DatabaseInfo(data[index:], rdb)
	index += indexOffset

	// Finally. Once all that is gone, what remains is key value pairs
	rdb, indexOffset = _parseRDB_KeyValue(data[index:], rdb)
	index += indexOffset

	return rdb
}

// Decodes the next string from string encoded bits,
// returns it's value, it's length(from the bytes for offset by the caller), and error if there is one.
func decodeStringEncoding(data []byte) (str string, strLength int, err error) {
	// Parse for strings
	// Strings have max length of 13 (0x0D). Anything with the first byte less than that is a string
	if data[0] <= stringEncoding_string {
		strLength := int(data[0]) + 1
		return string(data[1:strLength]), strLength, nil
	}

	// Parse for ints
	// Can be int8, int16 or int32. int64 is not supported
	switch data[0] {
	case stringEncoding_int8:
		return string(data[1]), 2, nil // 8bit int is one byte

	case stringEncoding_int16:
		val := binary.LittleEndian.Uint16(data[1:3])
		return string(val), 3, nil

	case stringEncoding_int32:
		val := binary.LittleEndian.Uint32(data[1:5])
		return string(val), 5, nil

	case stringEncoding_int64:
		return "", 0, fmt.Errorf("64 bit int not supported")
	}

	// If not int or string...
	return "", 0, fmt.Errorf("error decoding rdb string: tf did you provide")
}

// Decodes size-encoded bits. Returns the decoded value, index offset, error(if exists).
func decodeSizeEncoding(data []byte) (size string, indexOffset int, err error) {
	firstTwoBits := data[0] >> 6 // data[0] is one byte. need the first two BITS

	switch firstTwoBits {
	case sizeEncoding_6bits: // 6bit length, just need the remaining bits from the first byte.
		val := int(data[0] & 0x3F) // masking out the first two
		return string(val), 1, nil

	case sizeEncoding_14bits: // so i need first 6 bits, plus the next 8 bits
		// Now this is too complicated for my monkey brain. what follows is code with help from gpt
		val := int(data[0] & 0x3F)      // masking out the first two bits, apparently
		val = (val << 8) + int(data[1]) // shifting the value to the left by 8 and adding the second byte. Yes i dont understand this bit one bit.
		return string(val), 2, nil

	case sizeEncoding_idkwhattocallthis: // 5 byte length. Much more reasonable.
		val := binary.BigEndian.Uint32(data[1:6]) // give it the first 5 bytes
		return string(val), 3, nil

	case sizeEncoding_stringEncoding:
		return decodeStringEncoding(data)

	}

	// There are others, not handling those for this project
	return "", 0, fmt.Errorf("error decoding size encoding: no bloody clue what went wrong, come debug lil man")
}

// Decodes Expiry timestamp from ms or s. Returns the decoded value, index offset, error(if exists).
func decodeExpiryTimestamp(data []byte) (timestamp time.Time, indexOffset int, err error) {
	switch data[0] {
	case opCodeExpireTime: // next 4 bytes are unix timestamp (uint)
		rawTime := int64(binary.LittleEndian.Uint32(data[1:5]))
		timeStamp := time.Unix(rawTime, 0).UTC()
		return timeStamp, 5, nil

	case opCodeExpireTimeMs: // next 8 bytes are unix timestamp (ulong)
		rawTime := int64(binary.LittleEndian.Uint64(data[1:9]))
		timeStamp := time.UnixMilli(rawTime).UTC()
		return timeStamp, 9, nil

	default: // not a timestamp
		return time.Time{}, 0, fmt.Errorf("error decoding timestamp: provided bytes do not describe a timestamp (in s or ms)")
	}
}

// Loads the .rdb file from the given name and directory
func loadRDBFromFile(dir string, dbFileName string) ([]byte, bool) {
	data, err := os.ReadFile(dir + "/" + dbFileName)
	if err != nil {
		fmt.Println("error reading rdb file. Proceeding anyway...")
		return data, false
	}

	hasData := len(data) > 0
	return data, hasData
}

// Parses the metadata section of the rdb
func _parseRDB_MetaData(data []byte, rdb RedisRDB) (_ RedisRDB, indexOffset int) {
	index := 0
	// Auxiliary Section: Reading them, not saving them for now
	for len(data) > index && data[index] == opCodeAux {
		index++ // skip the opcode

		key, offset, err := decodeStringEncoding(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		index += offset // move past the key

		value, offset, err := decodeStringEncoding(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		index += offset // move past the value

		fmt.Println("key value found: ", key, value) // Doing nothing with it for now
	}

	return rdb, index
}

// TODO: IMPLEMENT THIS (WHEN REQUIRED lol)
// Long one, has multiple subsections. Doing nothing with all of them, just "parsing" and skipping past them.
func _parseRDB_DatabaseInfo(data []byte, rdb RedisRDB) (_ RedisRDB, indexOffset int) {
	index := 0

	// Database selector section. Not supporting more than one, so not doing anything with it rn.
	if data[index] == opCodeSelectDB {
		index++

		_, valLength, err := decodeSizeEncoding(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		index += valLength

		// fmt.Println("Database Index: ", val)
	}

	// Resize subsection: No idea agian.
	if data[index] == opCodeResizeDB {
		index++

		_, indexOffset, err := decodeSizeEncoding(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		// fmt.Println("RESIZEDB VAL: ", val)
		index += indexOffset

		_, indexOffset, err = decodeSizeEncoding(data[index:])
		// fmt.Println("RESIZEDB VAL: ", val)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		index += indexOffset
	}
	return rdb, index
}

// parses the key-value pairs stored.
func _parseRDB_KeyValue(data []byte, rdb RedisRDB) (_ RedisRDB, indexOffset int) {
	index := 0

	for data[index] == 0x00 || data[index] == opCodeExpireTime || data[index] == opCodeExpireTimeMs {
		timeStamp := time.Time{}
		expiresFlag := false

		if data[index] == opCodeExpireTime || data[index] == opCodeExpireTimeMs { // handle the expiry timestamp
			// fmt.Println(data[index])
			t, offset, err := decodeExpiryTimestamp(data[index:])
			if err != nil {
				fmt.Println("error parsing key-value pair: error parsing timestamp")
				os.Exit(0)
			}

			timeStamp = t
			expiresFlag = true
			index += offset
		}

		index++
		key, indexOffset, err := decodeStringEncoding(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		// fmt.Println("KEY: ", key)
		index += indexOffset

		value, indexOffset, err := decodeStringEncoding(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		// fmt.Println("VALUE: ", value)
		index += indexOffset
		record := RedisRecord{
			value: value,
		}
		if expiresFlag {
			record.expiresAt = timeStamp
			record.expires = true
		}
		// fmt.Println("KEY: ", key, "VALUE: ", value, "EXPIRY: ", timeStamp)

		rdb.keyValueStore.db[key] = record
	}

	return rdb, index
}

func encodeRDB(_ RedisRDB) []byte {
	// encoding an empty RDB to send to the replica server

	// DEcode the hex representation of an empty rdb (until we write the encoding for the actual store here.)
	emptyDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	return emptyDB
}
