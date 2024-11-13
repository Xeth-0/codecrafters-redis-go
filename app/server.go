package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

var RDB = redisRDB{}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Not enough arguements provided. Using default values...")
	}
	// Get the directory and filename for the rdb store.
	dirFlag := flag.String("dir", "dump", "rdb store directory")
	dbFileNameFlag := flag.String("dbfilename", "dump.rdb", "rdb store filename")

	flag.Parse()

	dir := *dirFlag
	dbFileName := *dbFileNameFlag

	// Setup the listener on the port
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("DIR: ", dir, "DBFILENAME: ", dbFileName)

	// Setup the RDB
	RDB = setupRDB(dir, dbFileName)

	// Listen for connections.
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

		n, err := conn.Read(readBuffer) // Read the request.
		if err != nil {
			return
		}

		readBuffer = readBuffer[:n] // Cut off the empty bytes at the end

		fmt.Println("--Incoming bytes => ", readBuffer)

		// parse the resp request into a struct for easier manipulation
		respRequest, err := parseRESP(readBuffer)
		if err != nil {
			fmt.Println("Error parsing request")
			os.Exit(0)
		}

		commands, _ := extractCommandFromRESP(respRequest)
		response := constructResponse(commands)
		byteResponse := []byte(response)

		fmt.Println("--Outgoing bytes =>  ", byteResponse)

		conn.Write(byteResponse)
	}
}
