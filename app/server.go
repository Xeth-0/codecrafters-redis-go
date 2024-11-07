package main

import (
	"fmt"
	"net"
	"os"
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
	// fmt.Println("Connection: ", conn)
	defer conn.Close()

	for {
		bytesRead := make([]byte, 128)
		conn.SetDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := conn.Read(bytesRead)
		if err != nil { // nothing read or encountered an error.
			return
		}
	
		// Debugging incoming stream.
		fmt.Println("Incoming bytes =>", bytesRead)
	
		// Hardcoding the response for this stage (Parsing comes later).
		conn.Write([]byte("+PONG\r\n"))
	}
}
