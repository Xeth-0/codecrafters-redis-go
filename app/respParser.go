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


