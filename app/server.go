package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

var RDB = RedisRDB{}
var CONFIG = RedisConfig{}

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

	// Read stored RDB.
	RDB = setupRDB(CONFIG.rdbDir, CONFIG.rdbDbFileName)

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

		time.Sleep(100 * time.Millisecond)

		go handleConnection(masterConn, true) // start listening to propagation requests from master
	} else {
		CONFIG.masterReplOffset = 0
		CONFIG.masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	}

	// Giving the master a chance to sync up with this replica. 2 seconds might seem
	//  excessive, it is, but I can't seem to avoid race conditions otherwise.
	time.Sleep(2000 * time.Millisecond)

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

		go handleConnection(conn, false)
	}
}

// Go-routine to accept and respond to new connections. Keeps running to listen to
//
//	and keep the connection alive.
func handleConnection(conn net.Conn, isMasterConn bool) {
	defer conn.Close()

	for {
		readBuffer, err := readFromConnection(conn)
		if err != nil {
			continue // discard and continue
		}
		// fmt.Println("Incoming Request: ", string(readBuffer))

		respRequests, err := parseRESP(readBuffer)
		if err != nil {
			logAndExit("Error parsing request", err)
		}
		processRequests(respRequests, readBuffer, conn, isMasterConn)
	}
}

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
func processRequests(respRequests []RESP, readBuffer []byte, conn net.Conn, isMasterConn bool) {
	if isMasterConn { // update the offset
		CONFIG.masterReplOffset += len(readBuffer)
	}

	// address each request
	for _, respRequest := range respRequests {
		commands, _ := extractCommandFromRESP(respRequest)
		if len(commands) < 1 {
			continue
		}

		responses, _ := executeResp(commands, conn)
		if isMasterConn {
			if commands[0] == "ping" || commands[0] == "set" { // should send no response
				continue
			}
		} else {
			if commands[0] == "set" { // only one that propagates so far is set
				CONFIG.masterReplOffset += len(readBuffer)
				propagateCommands(readBuffer)
			}
		}
		if len(responses) < 1 { // nothing to respond to
			return
		}
		if err := sendResponse(responses, conn); err != nil {
			// TODO: Handle error
		}

	}
}

// Send requests to the replica servers.
func propagateCommands(request []byte) error {
	var e error = nil
	for _, replica := range CONFIG.replicas {
		bytesWritten, err := replica.conn.Write(request)
		replica.offset += bytesWritten
		if err != nil {
			e = err
		}
	}
	return e
}

// Write the responses to the client/server.
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
