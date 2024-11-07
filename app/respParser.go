package main

import (
	"fmt"
	"os"
	"strconv"
)

// Parsing of incoming and outgoing resp requests.

func parseRESP(packet []byte) (RESP, error) {
	if len(packet) == 0 { // no data in packet. return empty resp struct
		return RESP{}, fmt.Errorf("error parsing RESP Request: No data in packet")
	}

	resp := RESP{} // is the return value

	// Grab the type from the first byte in the packet (it always starts with the type)
	resp.respType = RESPType(packet[0])

	// TODO:  Going to assume we get the right type for now. HANDLE THIS LATER

	resp.RawBytes = packet
	resp.Data = packet[1:] // Actual contents of the resp, excl the type and crlf

	// Parse the request
	switch resp.respType {
	case RESPTypes.Integer: // Signed/Unsigned ints
		return _parseRESP_Integer(resp.RawBytes), nil
	case RESPTypes.Bulk: // Bulk strings, contain their length with the request
		return _parseRESP_Bulk(resp.RawBytes), nil
	case RESPTypes.String: // Plain strings
		return _parseRESP_String(resp.RawBytes), nil
	case RESPTypes.Error: // Error? not sure when this would happen
		return _parseRESP_Error(resp.RawBytes), nil
	case RESPTypes.Array: // Starting point for most commands. They are sent here as ARRAY resp commands.
		return _parseRESP_Array(resp.RawBytes), nil
	}

	return RESP{}, fmt.Errorf("error parsing RESP Request: IDK")
}

func _parseRESP_Integer(respBytes []byte) RESP {
	resp := RESP{}
	resp.respType = RESPTypes.Integer

	sign := 1
	start := 0
	if respBytes[0] == '-' {
		sign = -1
		start++
	}

	// Iterate till crlf (integer might be more than one digit)
	for end := 0; end < len(respBytes); end++ {
		if respBytes[end] == '\r' && respBytes[end+1] == '\n' {
			intStr := string(respBytes[start:end])

			val, err := strconv.Atoi(intStr)
			if err != nil {
				fmt.Println("Error parsing int: Error parsing array of ints")
				os.Exit(0)
			}

			resp.respData.Int = sign * val
			resp.RawBytes = respBytes[:end+2] // Including the \r\n

			return resp
		}
	}
	return RESP{}
}

func _parseRESP_String(respBytes []byte) RESP {
	// TODO
	return RESP{}
}

func _parseRESP_Error(respBytes []byte) RESP {
	// TODO
	return RESP{}
}

func _parseRESP_Array(respBytes []byte) RESP {
	arrayLength := int(respBytes[1] - '0')

	resp := RESP{}
	resp.Length = arrayLength
	resp.respType = RESPTypes.Array

	p := 4 // skipping past the \r\n after the length.p := 0

	for ; arrayLength >= 1; arrayLength-- {
		if p >= len(respBytes) {
			fmt.Println("Error parsing array: Unexpected end of resp request.")
			os.Exit(0)
		}

		subResp, err := parseRESP(respBytes[p:])
		if err != nil {
			fmt.Println("Error parsing array: Error parsing array elements.")
		}

		resp.respData.Array = append(resp.respData.Array, subResp)
		p += len(subResp.RawBytes)
	}

	resp.RawBytes = respBytes[:p]
	return resp
}

func _parseRESP_Bulk(respBytes []byte) RESP {
	stringLength := int(respBytes[1] - '0')

	resp := RESP{}
	resp.respType = RESPTypes.Bulk
	resp.Length = stringLength

	resp.respData.String = string(respBytes[4 : 4+stringLength])
	resp.RawBytes = respBytes[:6+stringLength]

	return resp
}

// func _parseNextRESP(respBytes []byte) int {
// 	// Iterates until it finds the end of the current RESP request.
// 	// Tf am i gonna do if it's an array. aaaaaaaaand got it

// 	switch RESPType(respBytes[0]) {
// 	case RESPTypes.Array: // doesn't go until first the crlf like the others.
// 		arrayLength := int(respBytes[1] - '0')
// 		p := 3 // skipping past the \r\n after the length.

// 		for ; arrayLength >= 1; arrayLength-- { // until we've traversed all the elements of the array.
// 			if p >= len(respBytes) {
// 				fmt.Println("Error parsing array: reached the end of the request bytes but there are undiscovered array elements")
// 				os.Exit(0)
// 			}

// 		}
// 		return p
// 	case RESPTypes.Integer:

// 	case RESPTypes.Bulk:

// 	case RESPTypes.String:

// 	case RESPTypes.Error:
// 	}

// 	return -1 // backup
// }

// func parseRequest(packet []byte) {
// 	// requestString := string(packet)
// 	start := 1

// 	for {
// 		sectionLen, subsection := something(packet[start:])
// 		if sectionLen == 0 && subsection == 0{
// 			// End of packet?
// 			break
// 		}
// 		start ++
// 		if packet[start] == '\r' && packet[start + 1] == '\n'{
// 			start = sectionLen + 3
// 		}
// 	}
// }
// func something(packet []byte) (int, int) {
// 	length := 0
// 	i := 0

// 	for i < len(packet) {
// 		if packet[i] == '\r' {
// 			break
// 		}

// 		length = (length * 10) + i + 1
// 		i++
// 	}

// 	return length, i
// }
