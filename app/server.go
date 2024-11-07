package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection")
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		readBuffer := make([]byte, 128)

		// Deadline to stop listening to read/write from the tcp connection
		conn.SetDeadline(time.Now().Add(100 * time.Millisecond))

		// Read the request.
		len, err := conn.Read(readBuffer)
		if err != nil { // nothing read or encountered an error.
			return
		}

		// Cut off the empty bytes at the end
		readBuffer = readBuffer[:len]

		// Debugging incoming stream.
		fmt.Println("Incoming bytes =>", readBuffer)

		// fmt.Println("Incoming bytes =>", string(bytesRead))
		respRequest, err := parseRESP(readBuffer)
		if err != nil {
			fmt.Println("Error parsing request")
			os.Exit(0)
		}

		commands, length := extractCommandFromRESP(respRequest)

		// Hardcoding the response for this stage (Parsing comes later).
		if commands[0] == "ping" {
			conn.Write([]byte("+PONG\r\n"))
		}

		if commands[0] == "echo" {
			arg := ""
			if length >= 1 {
				arg = commands[1]
			}
			conn.Write([]byte("+" + arg + "\r\n"))
		}

	}
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
