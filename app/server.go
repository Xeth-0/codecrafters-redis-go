package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

var RDB = redisRDB{}
var CONFIG = redisConfig{}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Not enough arguements provided. Using default values for missing fields...")
	}

	// Get the directory and filename for the rdb store.
	dirFlag := flag.String("dir", "dump", "rdb store directory")
	replicaOfFlag := flag.String("replicaof", "master", "master or slave")
	dbFileNameFlag := flag.String("dbfilename", "dump.rdb", "rdb store filename")
	portFlag := flag.Int("port", 6379, "the port that this redis server will use to run")

	flag.Parse()

	dir := *dirFlag
	port := *portFlag
	replicaOf := *replicaOfFlag
	dbFileName := *dbFileNameFlag

	CONFIG.port = port
	CONFIG.rdbDir = dir
	CONFIG.rdbDbFileName = dbFileName

	if replicaOf != "master" { // has to be a replica, with the master's ip and port provided in 'replicaof'
		CONFIG.isSlave = true
		r := strings.Split(replicaOf, " ")
		if len(r) < 2 {
			fmt.Println("invalid replica flag. try agian")
			os.Exit(0)
		}
		CONFIG.masterHost = r[0]
		CONFIG.masterPort = r[1]

		masterConn := connectToMaster()

		go handleConnection(masterConn) // start listening to propagation requests from master

	} else {
		CONFIG.masterReplOffset = 0
		CONFIG.masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	}

	// Read stored RDB.
	RDB = setupRDB(CONFIG.rdbDir, CONFIG.rdbDbFileName) 

	// Start the server and begin listening to tcp connections for clients.
	startServer()
}

// Setup the tcp listener on the port specified
func startServer() {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", CONFIG.port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	// Listen for connections.
	for { 
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
			continue // discard current connection and continue
		}

		go handleConnection(conn)
	}
}

// Go-routine to accept and respond to new connections. Keeps running to listen to 
//   and keep the connection alive.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		readBuffer := make([]byte, 1024)
		n, err := conn.Read(readBuffer) // Read the request.
		if err != nil {
			return
		}
		// Cut off the empty bytes at the end
		readBuffer = readBuffer[:n]
		fmt.Println("--Incoming bytes => ", readBuffer)
		fmt.Println("--Incoming req => ", string(readBuffer))

		// parse the resp request into a struct for easier manipulation
		respRequests, err := parseRESP(readBuffer)
		if err != nil {
			fmt.Println("Error parsing request")
			os.Exit(0)
		}

		for _, respRequest := range respRequests {
			// Extract the commands sent from the resp request.
			commands, _ := extractCommandFromRESP(respRequest)
			switch commands[0] {
			case "set":
				propagateCommands(readBuffer)
			}
			
			// Construct and send the response for the resp request using the given commands.
			responses, _ := executeResp(commands, conn)
	
			err = sendResponse(responses, conn)
			if err != nil {
				// TODO
			}
		}
	}
}

// func handleConnection(conn net.Conn) {
// 	defer conn.Close()

// 	for {
// 		readBuffer, err := readFromConnection(conn)
// 		if err != nil {
// 			continue // discard and continue
// 		}

// 		respRequests, err := parseRESP(readBuffer)
// 		if err != nil {
// 			logAndExit("Error parsing request", err)
// 		}

// 		processRequests(respRequests, readBuffer, conn)
// 	}
// }

// Reads from the given connection into a buffer. returns the buffer.
func readFromConnection(conn net.Conn) ([]byte, error) {
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer[:n], nil
}

// Handles the read request and responds to the request.
func processRequests(respRequests []RESP, readBuffer []byte, conn net.Conn) {
	for _, respRequest := range respRequests {
		commands, _ := extractCommandFromRESP(respRequest)

		if commands[0] == "set" { // only one that propagates so far is set
			propagateCommands(readBuffer)
		}

		responses, _ := executeResp(commands, conn)
		if err := sendResponse(responses, conn); err != nil {
			// TODO: Handle error
		}
	}
}

func propagateCommands(request []byte) error {
	var e error = nil
	for _, replica := range CONFIG.replicas {
		_, err := replica.Write(request)
		if err != nil {
			e = err
		}
	}
	return e
}

func sendResponse(responses []string, conn net.Conn) error {
	for _, response := range responses {
		conn.Write([]byte(response))
	}
	return nil
}

// Logs the error and message and exits with os.Exit(0).
func logAndExit(message string, err error) {
	fmt.Println(message, ":", err)
	os.Exit(0)
}
