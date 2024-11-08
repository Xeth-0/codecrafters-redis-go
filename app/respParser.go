package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
		return _parseRESP_Integer(resp.RawBytes)
	case RESPTypes.Bulk: // Bulk strings, contain their length with the request
		return _parseRESP_Bulk(resp.RawBytes)
	case RESPTypes.String: // Plain strings
		return _parseRESP_String(resp.RawBytes)
	case RESPTypes.Error: // Error? not sure when this would happen
		return _parseRESP_Error(resp.RawBytes)
	case RESPTypes.Array: // Starting point for most commands. They are sent here as ARRAY resp commands.
		return _parseRESP_Array(resp.RawBytes)
	}

	return RESP{}, fmt.Errorf("error parsing RESP Request: IDK")
}

func _parseRESP_Integer(respBytes []byte) (RESP, error) {
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
			return resp, nil
		}
	}
	return RESP{}, fmt.Errorf("error parsing int resp: unexpected error") // should never hit this.
}

func _parseRESP_String(respBytes []byte) (RESP, error) {
	// TODO
	return RESP{}, fmt.Errorf("error parsing string resp: unexpected error") // should never hit this
}

func _parseRESP_Error(respBytes []byte) (RESP, error) {
	// TODO
	return RESP{}, fmt.Errorf("error parsing error resp: unexpected error") // should never hit this
}

func _parseRESP_Array(respBytes []byte) (RESP, error) {
	// Determine the array length from the request.
	clrf := 1 // find the clrf
	for ; clrf < len(respBytes); clrf++ {
		if respBytes[clrf] == '\r' && respBytes[clrf+1] == '\n' { // Here it is
			break
		}
	}

	// Parse it to extract the array length
	arrayLength, err := strconv.Atoi(string(respBytes[1:clrf]))
	if err != nil {
		return RESP{}, fmt.Errorf("error parsing array: cannot parse the length provided")
	}

	// Construct the response. Yes this is a bit ahead of time.
	resp := RESP{}
	resp.Length = arrayLength
	resp.respType = RESPTypes.Array

	// Iterate over the array elements to extract the values
	p := clrf + 2 // skipping past the \r\n after the length.p := 0

	for ; arrayLength >= 1; arrayLength-- {
		if p >= len(respBytes) {
			err := fmt.Errorf("error parsing array: Unexpected end of resp request")
			fmt.Println(err)
			return RESP{}, err
		}

		subResp, err := parseRESP(respBytes[p:])
		if err != nil {
			fmt.Println("Error parsing array: Error parsing array elements.")
			return RESP{}, err
		}

		resp.respData.Array = append(resp.respData.Array, subResp)
		p += len(subResp.RawBytes)
	}

	resp.RawBytes = respBytes[:p]
	return resp, nil
}

func _parseRESP_Bulk(respBytes []byte) (RESP, error) {
	// Extract the Bulk String length
	p := 1
	for ; p < len(respBytes); p++ {
		if respBytes[p] == '\r' && respBytes[p+1] == '\n' {
			break
		}
	}

	stringLength, err := strconv.Atoi(string(respBytes[1:p]))
	if err != nil {
		return RESP{}, fmt.Errorf("error parsing bulk string resp: cannot parse the length provided")
	}

	if stringLength > len(respBytes) {
		err := fmt.Errorf("error parsing bulk string resp: invlaid string length provided")
		return RESP{}, err
	}

	// Start parsing the actual string.
	stringStart := p + 2

	resp := RESP{}
	resp.respType = RESPTypes.Bulk
	resp.Length = stringLength

	resp.respData.String = string(respBytes[stringStart : stringStart+stringLength])
	resp.RawBytes = respBytes[:stringStart+stringLength+2]

	return resp, nil
}

func extractCommandFromRESP(resp RESP) ([]string, int) {
	arr := resp.respData.Array

	ret := make([]string, len(arr))

	for i, subresp := range arr {
		val := strings.ToLower(subresp.respData.String)
		ret[i] = val
	}

	return ret, len(ret)
}
