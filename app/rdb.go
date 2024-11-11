package main

import (
	"encoding/binary"
	"fmt"
	"os"
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
func setupRDB(dir string, dbFileName string) redisRDB {
	rdbConfig := RDBConfig{
		dir:        dir,
		dbFileName: dbFileName,
	}
	rdb := redisRDB{
		config: rdbConfig,
	}
	rdb.databaseStore = make(map[string]string)

	// need to load in the rdb specified by the dirname and dir.
	data := loadRDB(dir, dbFileName)

	// and now, for the looooooooong process of loading the bitch
	index := 0

	// skip the header (Magic String + Version Number => "REDIS0012")
	index += 9

	// TODO: Separate each of these into their own function. Should be easy enough.
	// Auxiliary Section: Reading them, not saving them for now
	for data[index] == opCodeAux && len(data) > index {
		index++

		key, keyLength, err := decodeNextString(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		index += keyLength

		value, valueLength, err := decodeNextString(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		index += valueLength

		fmt.Println("key value found: ", key, value) // Doing nothing with it for now
	}

	// Database Index. Not supporting more than one
	if data[index] == opCodeSelectDB {
		index++

		val, valLength, err := decodeSize(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		index += valLength

		// again, doing nothing with this
		fmt.Println("Database Index: ", val)
	}

	// Resize section: No idea agian.
	// TODO: Actually implement this?
	if data[index] == opCodeResizeDB {
		index++

		val, indexOffset, err := decodeSize(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		fmt.Println("RESIZEDB VAL: ", val)
		index += indexOffset

		val, indexOffset, err = decodeSize(data[index:])
		fmt.Println("RESIZEDB VAL: ", val)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		index += indexOffset
	}

	// Database content. Not even parsing this, just skipping it for now.
	for data[index] != opCodeEOF {
		if data[index] == opCodeExpireTime {
			index++
			// TODO
			index += 4
		} else if data[index] == opCodeExpireTimeMs {
			index++
			// TODO
			index += 8
		} else {
			break
		}
	}

	// Finally. Once all that is gone, what remains is key value pairs
	for data[index] == 0x00 {
		index++
		key, indexOffset, err := decodeNextString(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		fmt.Println("KEY: ", key)
		index += indexOffset

		value, indexOffset, err := decodeNextString(data[index:])
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		fmt.Println("VALUE: ", value)
		index += indexOffset

		rdb.databaseStore[key] = value
	}

	//
	return rdb
}

// Decodes the next string from string encoded bits,
// returns it's value, it's length(from the bytes for offset by the caller), and error if there is one.
func decodeNextString(data []byte) (str string, strLength int, err error) {
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

/*
If the first two bits are 0b00:

	The size is the remaining 6 bits of the byte.
	In this example, the size is 10:
	0A
	00001010

If the first two bits are 0b01:

	The size is the next 14 bits
	(remaining 6 bits in the first byte, combined with the next byte),
	in big-endian (read left-to-right).
	In this example, the size is 700:
		42 BC
		01000010 10111100

If the first two bits are 0b10:

	Ignore the remaining 6 bits of the first byte.
	The size is the next 4 bytes, in big-endian (read left-to-right).
	In this example, the size is 17000:
		80 00 00 42 68
		10000000 00000000 00000000 01000010 01101000

If the first two bits are 0b11:

	The remaining 6 bits specify a type of string encoding.
	See string encoding section.
*/
func decodeSize(data []byte) (size string, sizeLength int, err error) {
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
		return decodeNextString(data)

	}

	// There are others, not handling those for this project
	return "", 0, fmt.Errorf("error decoding size encoding: no bloody clue what went wrong, come debug lil man")
}

// Loads the .rdb file from the given name and directory
func loadRDB(dir string, dbFileName string) []byte {
	fullPath := dir + "/" +dbFileName
	fmt.Println(fullPath)
	data, _ := os.ReadFile(fullPath)
	fmt.Println(data)
	// if err != nil {
	// 	fmt.Println("Error reading rdb file.")
	// }
	fmt.Println(string(data))
	return data
}
