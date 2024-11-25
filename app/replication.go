package main

import (
	"fmt"
	"net"
	"time"
)

// Sets up a connection to the master server and performs the replication handshake.
//   Returns the connection to the master.
func connectToMaster() net.Conn {
	address := fmt.Sprintf("%s:%s", CONFIG.masterHost, CONFIG.masterPort)
	masterConn, err := net.Dial("tcp", address)
	if err != nil {
		logAndExit("error sending handshake to master: cannot establish dialup with the master", err)
	}

	sendHandshake(masterConn)
	return masterConn
}

// Performs the handshake steps to the master on the given connection.
func sendHandshake(conn net.Conn) error {
	fmt.Println("Establishing handshake with master...")
	
	_handshakeSendPing(conn, []string{"PING"}, "PONG")
	_handshakeSendReplConf(conn, "listening-port", fmt.Sprintf("%d", CONFIG.port))
	_handshakeSendReplConf(conn, "capa", "psync2")
	_handshakeSendPsync(conn)

	fmt.Println("Handshake Established.")
	return nil
}


func _handshakeSendPing(conn net.Conn, command []string, expectedResponse string) {
	conn.Write([]byte(respEncodeStringArray(command)))
	time.Sleep(10 * time.Millisecond)

	responseBuffer := make([]byte, 1024)
	_, err := conn.Read(responseBuffer)
	if err != nil {
		logAndExit("error receiving handshake response from master", err)
	}

	response, err := _parseRESP(responseBuffer)
	if err != nil || response.respData.String != expectedResponse {
		logAndExit(fmt.Sprintf("error receiving handshake response: expected %s", expectedResponse), err)
	}
}

func _handshakeSendReplConf(conn net.Conn, key, value string) {
	replConfReq := []string{"REPLCONF", key, value}
	conn.Write([]byte(respEncodeStringArray(replConfReq)))
	time.Sleep(10 * time.Millisecond)

	responseBuffer := make([]byte, 1024)
	_, err := conn.Read(responseBuffer)
	if err != nil {
		logAndExit("error receiving handshake response from master(replconf)", err)
	}
	// should also be checking that the response is +OK\r\n, but sometimes the server
	// will send nothing, and wait to send the entire thing at once. This works for now.

	fmt.Println("handshake response(replconf):", string(responseBuffer))
}

func _handshakeSendPsync(conn net.Conn) {
	psyncReq := []string{"PSYNC", "?", "-1"}
	conn.Write([]byte(respEncodeStringArray(psyncReq)))
	time.Sleep(100 * time.Millisecond)

	responseBuffer := make([]byte, 1024)
	_, err := conn.Read(responseBuffer)
	if err != nil {
		logAndExit("error receiving handshake response from master", err)
	}

	// should also be checking that the response is the rdb, but sometimes the server
	// will send nothing, and wait to send the entire thing at once. This works for now.'

	fmt.Println("handshake response (psync):", string(responseBuffer))
	resp := respEncodeStringArray([]string{"REPLCONF", "ACK", "0"})
	conn.Write([]byte(resp))
}
