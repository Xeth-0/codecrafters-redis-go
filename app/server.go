package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

// What it says it is.
var keyValueStore = map[string]string{}

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
		readBuffer := make([]byte, 1024)

		// Deadline to stop listening to read/write from the tcp connection
		conn.SetDeadline(time.Now().Add(100 * time.Millisecond))

		// Read the request.
		n, err := conn.Read(readBuffer)
		if err != nil { // nothing read or encountered an error.
			return
		}

		// Cut off the empty bytes at the end
		readBuffer = readBuffer[:n]

		// Debugging incoming stream.
		fmt.Println("Incoming bytes =>", readBuffer)

		// fmt.Println("Incoming bytes =>", string(bytesRead))
		respRequest, err := parseRESP(readBuffer)
		if err != nil {
			fmt.Println("Error parsing request")
			os.Exit(0)
		}

		commands, _ := extractCommandFromRESP(respRequest)

		response := constructResponse(commands)
		conn.Write([]byte(response))
	}
}

func constructResponse(commands []string) string {
	for i := 0; i < len(commands); i += 2 {
		switch commands[0] {
		case "ping":
			return "+PONG\r\n"
		case "echo":
			arg := ""
			if len(commands) >= 1 {
				arg = commands[i+1]
			}
			return fmt.Sprintf("+%s\r\n", arg)
		case "set":
			keyValueStore[commands[i+1]] = commands[i+2]
			return "+OK\r\n"
		case "get":
			val, exists := keyValueStore[commands[i+1]]
			if exists {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			}
			return ""
		}
	}
	return "-ERROR"
}
