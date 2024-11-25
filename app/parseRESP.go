package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Parse incoming resp request. Returns an array of resp objects containing the 
// parsed requests. This is because sometimes more than one request might be bundled
// into the same tcp request.
func parseRESP(respBytes []byte) ([]RESP, error) {
	fmt.Println(string(respBytes))
	resps := make([]RESP, 0, 2)
	for len(respBytes) > 0 {
		resp, err := _parseRESP(respBytes)
		if err != nil {
			return resps, err
		}
		resps = append(resps, resp)
		if (len(resp.RawBytes) >= len(respBytes)){
			break
		}
		respBytes = respBytes[len(resp.RawBytes):]
	}
	return resps, nil
}

// Parse individual resp requests, returns one resp object containing the parsed values.
func _parseRESP(respBytes []byte) (RESP, error) {
	if len(respBytes) == 0 { // no data in packet. return empty resp struct
		return RESP{}, fmt.Errorf("error parsing RESP Request: No data in packet")
	}

	// Grab the type from the first byte in the packet (it always starts with the type)
	respType := RESPType(respBytes[0])

	// Parse the request baesd on the type
	switch respType {
	case RESPTypes.Integer: // Signed/Unsigned ints
		return _parseRESP_Integer(respBytes)
	case RESPTypes.Bulk: // Bulk strings, contain their length with the request
		return _parseRESP_Bulk(respBytes)
	case RESPTypes.String: // Plain strings
		return _parseRESP_String(respBytes)
	case RESPTypes.Error: // Error
		return _parseRESP_Error(respBytes)
	case RESPTypes.Array: // Starting point for most commands. They are sent here as ARRAY resp commands.
		return _parseRESP_Array(respBytes)
	}

	return RESP{}, fmt.Errorf("error parsing RESP Request: IDK: %s", string(respBytes))
}

func _parseRESP_Integer(respBytes []byte) (RESP, error) {
	resp := RESP{}
	resp.respType = RESPTypes.Integer

	sign := 1
	start := 0

	if respBytes[0] == '-' { // Negative integer
		sign = -1
		start++ // move past the sign
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
	crlf, err := findNextCLRF(respBytes)
	if err != nil {
		return RESP{}, err
	}

	resp := RESP{} // setup response

	resp.respType = RESPTypes.String
	resp.RawBytes = respBytes[:crlf+2]
	resp.respData.String = string(respBytes[1:crlf])

	return resp, nil
}

func _parseRESP_Error(respBytes []byte) (RESP, error) {
	crlf, err := findNextCLRF(respBytes)
	if err != nil {
		return RESP{}, err
	}

	resp := RESP{} // setup response

	resp.respType = RESPTypes.Error
	resp.RawBytes = respBytes[:crlf+2]
	resp.respData.String = string(respBytes[1:crlf])

	return resp, nil
}

func _parseRESP_Array(respBytes []byte) (RESP, error) {
	// Determine the array length from the request.
	clrf, err := findNextCLRF(respBytes)
	if err != nil {
		fmt.Println("error parsing array: CLRF for length identifier not found")
		return RESP{}, err
	}

	// Parse it to extract the array length
	arrayLength, err := strconv.Atoi(string(respBytes[1:clrf]))
	if err != nil {
		return RESP{}, fmt.Errorf("error parsing array: cannot parse the length provided")
	}

	// Construct the response.
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

		subResp, err := _parseRESP(respBytes[p:])
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
	clrf, err := findNextCLRF(respBytes)
	if err != nil {
		fmt.Println("error parsing bulk string: CLRF after string length not found")
		return RESP{}, err
	}

	stringLength, err := strconv.Atoi(string(respBytes[1:clrf]))
	if err != nil {
		return RESP{}, fmt.Errorf("error parsing bulk string resp: cannot parse the length provided")
	}

	if stringLength > len(respBytes) {
		err := fmt.Errorf("error parsing bulk string resp: invlaid string length provided")
		return RESP{}, err
	}

	// Start parsing the actual string.
	stringStart := clrf + 2

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

func findNextCLRF(b []byte) (int, error) {
	for clrf := 0; clrf < len(b); clrf++ {
		if b[clrf] == '\r' && b[clrf+1] == '\n' { // Here it is
			return clrf, nil
		}
	}
	return -1, fmt.Errorf("no crlf found")
}
