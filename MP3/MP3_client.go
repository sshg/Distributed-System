package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	// HARD CODE
	serverA = "10.192.186.30:4444"
	serverB = "10.192.186.30:5555"
	serverC = "10.192.186.30:6666"
	serverD = "10.192.186.30:7777"
	serverE = "10.192.186.30:8888"
	coordinator = "10.192.186.30:9999" //coordinator IP
	coButton string
	abortFlag = false
)

var (
	connList map[string] string
	tcpList map[string]*net.TCPConn
	curList []string

	clientReadChan chan string
	clientWriteChan chan string
)


func globalInit() {
	connList = make(map[string] string)
	tcpList = make(map[string]*net.TCPConn)

	clientReadChan = make(chan string, 100)
	clientWriteChan = make(chan string, 100)

	// HARD CODE
	connList["A"] = serverA
	connList["B"] = serverB
	connList["C"] = serverC
	connList["D"] = serverD
	connList["E"] = serverE
	connList["CO"] = coordinator
}

func inArray(array []string, key string) bool {

	for _, val := range array {
		if val == key {
			return true
		}
	}
	return false
}

func startClient() {

	go clientWrite()

	for {
		inputReader := bufio.NewReader(os.Stdin)
		writeData, err := inputReader.ReadString('\n')
		checkError(err)
		clientWriteChan <- writeData
	}
}

func clientRead(conn net.Conn) {

	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		read_len, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error to read message because of ", err)
			break
		}

		readStr := string(buf[0:read_len])

		readDataList := strings.Split(readStr, "\n")

		for _, readData := range readDataList {
			if readData == "ABORTED" || readData == "NOT FOUND" || readData == "COMMIT OK"{
				if !abortFlag {
					fmt.Println(readData)
				}
				abortFlag = true
				continue
			}

			fmt.Println(readData)
		}
	}
}

func clientWrite() {

	for {
		rawIn := <- clientWriteChan

		inStr := strings.Split(rawIn, "\n")

		abortFlag = false

		for _, writeStr := range(inStr) {

			splitWords := strings.Split(writeStr, " ")

			switch splitWords[0] {

			case "BEGIN":
				fmt.Println("OK")
				continue

			case "SET", "GET":
				block := strings.Split(splitWords[1], ".")

				if !inArray(curList, block[0]) {
					curList = append(curList, block[0])
				}

				if conn, ok := tcpList[block[0]]; ok {
					_, err := conn.Write([]byte(writeStr))

					if err != nil {
						fmt.Println("Error to write message because of ", err)
					}
				} else {
					tcpAddr, err := net.ResolveTCPAddr("tcp4", connList[block[0]])
					checkError(err)
					conn, err := net.DialTCP("tcp", nil, tcpAddr)
					checkError(err)
					_, err = conn.Write([]byte(writeStr))

					if err != nil {
						fmt.Println("Error to write message because of ", err)
					}

					tcpList[block[0]] = conn

					go clientRead(conn)
				}

				// send coordinator
				if coButton == "1" {

					curIp := tcpList[block[0]].LocalAddr().String()
					msg := "CLIENT||" + curIp + "||" + splitWords[1] + "||"

					if splitWords[0] == "SET" {
						msg += "W\n"
					} else {
						msg += "R\n"
					}

					_, err := tcpList["CO"].Write([]byte(msg))
					fmt.Println(curIp)

					if err != nil {
						fmt.Println("Error to write coordinator msg ", err)
					}
				}

			case "COMMIT":
				if len(curList) == 0 {
					fmt.Println("COMMIT FAIL! NO TRANSACTIONS")
				}
				for _, resource := range curList {

					_, err := tcpList[resource].Write([]byte("COMMIT\n"))
					checkError(err)

					if coButton == "1" {

						curIp := tcpList[resource].LocalAddr().String()

						msg := "COMMIT||" + curIp + "\n"

						_, err := tcpList["CO"].Write([]byte(msg))

						if err != nil {
							fmt.Println("Error to write coordinator msg ", err)
						}
					}
				}
				curList = curList[:0]


			case "ABORT":
				if len(curList) == 0 {
					fmt.Println("ABORT FAIL! NO TRANSACTIONS")
				}
				for _, resource := range curList {
					_, err := tcpList[resource].Write([]byte("ABORT\n"))
					checkError(err)

					if coButton == "1" {

						curIp := tcpList[resource].LocalAddr().String()

						msg := "ABORT||" + curIp + "\n"

						_, err := tcpList["CO"].Write([]byte(msg))

						if err != nil {
							fmt.Println("Error to write coordinator msg ", err)
						}
					}
				}
				curList = curList[:0]
			}
		}
	}
}

func getOwnIP() []string {
	var res []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops:" + err.Error())
		os.Exit(1)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				res = append(res, ipnet.IP.String())

			}
		}
	}
	return res
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func main() {

	// coordinator
	//tcpAddr, err := net.ResolveTCPAddr("tcp4", connList["CO"])
	//checkError(err)
	//coConn, err := net.DialTCP("tcp", nil, tcpAddr)
	//checkError(err)

	if len(os.Args) != 2 {
		fmt.Println(os.Stderr, "Incorrect number of parameters, suppose to have {coordinator} 0 is off, 1 is on")
		fmt.Println(len(os.Args))
		os.Exit(1)
	}

	globalInit()

	coButton = os.Args[1]

	fmt.Println(coButton)

	if coButton == "1" {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", connList["CO"])
		checkError(err)
		coConn, err := net.DialTCP("tcp", nil, tcpAddr)
		checkError(err)
		tcpList["CO"] = coConn
	}
	//fmt.Println(getOwnIP())

	startClient()

	// for {}
}