package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

var RDB = redisRDB{}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Not enough arguements provided. Using default values for missing fields...")
	}
	// Get the directory and filename for the rdb store.
	dirFlag := flag.String("dir", "dump", "rdb store directory")
	dbFileNameFlag := flag.String("dbfilename", "dump.rdb", "rdb store filename")
	portFlag := flag.Int("port", 6379, "the port that this redis server will use to run")

	flag.Parse()

	dir := *dirFlag
	dbFileName := *dbFileNameFlag
	port := *portFlag

	// Setup the listener on the port
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

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

		// fmt.Println("--Incoming bytes => ", readBuffer)

		// parse the resp request into a struct for easier manipulation
		respRequest, err := parseRESP(readBuffer)
		if err != nil {
			fmt.Println("Error parsing request")
			os.Exit(0)
		}

		commands, _ := extractCommandFromRESP(respRequest)
		response := constructResponse(commands)
		byteResponse := []byte(response)

		// fmt.Println("--Outgoing bytes =>  ", byteResponse)

		conn.Write(byteResponse)
	}
}
