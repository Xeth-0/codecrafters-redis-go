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
	dbFileNameFlag := flag.String("dbfilename", "dump.rdb", "rdb store filename")
	portFlag := flag.Int("port", 6379, "the port that this redis server will use to run")
	replicaOfFlag := flag.String("replicaof", "master", "master or slave")

	flag.Parse()

	dir := *dirFlag
	dbFileName := *dbFileNameFlag
	port := *portFlag
	replicaOf := *replicaOfFlag

	fmt.Println(replicaOf)

	if replicaOf != "master" { // is a replica
		CONFIG.isSlave = true

		r := strings.Split(replicaOf, " ")
		if len(r) < 2 {
			fmt.Println("invalid replica flag. try agian")
			os.Exit(0)
		}

		CONFIG.masterHost = r[0]
		CONFIG.masterPort = r[1]
		// call the replica thingy

		sendHandshake()
	} else {
		// set the replication ID and offset
		CONFIG.masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		CONFIG.masterReplOffset = 0
	}

	CONFIG.port = port
	CONFIG.rdbDbFileName = dbFileName
	CONFIG.rdbDir = dir

	startServer()
}

func sendHandshake() {
	address := fmt.Sprintf("%s:%s", CONFIG.masterHost, CONFIG.masterPort)
	m, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("error sending handshake to master: ", err)
		os.Exit(0)
	}
	req := make([]string, 1)
	req[0] = "PING"
	m.Write([]byte(respEncodeStringArray(req)))
}

// Setup the tcp listener on the port specified
func startServer() {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", CONFIG.port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	// Setup the RDB
	RDB = setupRDB(CONFIG.rdbDir, CONFIG.rdbDbFileName)

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
