package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	serverName string
	portNum string
	localIp string
	coordinator = "10.192.186.30:9999"
	coButton string
	coConn *net.TCPConn
)
// RWmutex
var (
	// mutexMap map[string] *sync.RWMutex // x : mutexMap[x].RLock()/UnLock()
	mutexMap map[string] string // U: unlock, R: readlock, W: writeLock
	mutexMapLock = sync.RWMutex{}

	mutexMap4Ip map[string] (map[string] string) // record the R/W locks hold by client, x.x.x.x:5555 : {"x" : "R", "y": "W"}

	waitList map[string] []string // block -> 0
	waitListLock = sync.RWMutex{}
)

var (
	doneTrans map[string] string // block -> 3
	undoneTrans map[string] (map[string] int) // ip -> {"x":10, "y":5}
	doneTransLock = sync.RWMutex{}

	rcvChan map[string] (chan string)
	abortChan map[string] (chan bool)
	releaseChan map[string] (map[string] chan bool) // a -> ip -> chan
	releaseChanLock = sync.RWMutex{}
)

func globalInit() {
	//mutexMap = make(map[string] *sync.RWMutex)
	mutexMap = make(map[string] string)
	mutexMap4Ip = make(map[string] (map[string] string))
	doneTrans = make(map[string] string)
	undoneTrans = make(map[string] (map[string] int))

	rcvChan = make(map[string] (chan string))
	abortChan  = make(map[string] (chan bool))
	releaseChan = make(map[string] (map[string] chan bool))
	waitList = make(map[string] []string)
}

func removeElem(array []string, key string) []string {
	if len(array) == 0 {
		return array
	}
	for i, v := range array {
		if v == key {
			array = append(array[:i], array[i+1:]...)
			return array
		}
	}
	return array
}

func startServer() {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", localIp + ":"+ portNum)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	defer listener.Close()

	for {
		conn, err := listener.Accept()

		if err != nil {
			continue
		}
		fmt.Printf("Received message %s -> %s \n", conn.RemoteAddr(), conn.LocalAddr())

		go handleClient(conn)
	}
}

func serverRead(conn net.Conn) {

	for {
		//fmt.Print("hi")
		buf := make([]byte, 1024)
		read_len, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error to read message because of ", err)
			break
		}

		readData := string(buf[0:read_len])

		fmt.Println(readData)

		if readData == "ABORT\n" {
			if _, ok := abortChan[conn.RemoteAddr().String()]; ok {
				abortChan[conn.RemoteAddr().String()] <- true
				continue
			} else {
				abortChan[conn.RemoteAddr().String()] = make(chan bool)
				abortChan[conn.RemoteAddr().String()] <- true
			}
		}

		if _, ok := rcvChan[conn.RemoteAddr().String()]; ok {
			rcvChan[conn.RemoteAddr().String()] <- readData
		} else {
			rcvChan[conn.RemoteAddr().String()] = make(chan string, 100)
			rcvChan[conn.RemoteAddr().String()] <- readData
		}
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()  // close connection before exit
	// conn.SetReadDeadline(time.Now().Add(2 * time.Minute)) // set 2 minutes timeout
	curIp := conn.RemoteAddr().String()

	rcvChan[curIp] = make(chan string, 100)
	abortChan[curIp] = make(chan bool, 10)

	go serverRead(conn)

	for {
		select {

		case rawStr := <- rcvChan[curIp]:

			readStrList := strings.Split(rawStr, "\n")

			for _, readStr := range(readStrList) {
				splitWords := strings.Fields(readStr)
				if len(splitWords) == 0 {
					continue
				}
				fmt.Println("From: ", curIp)

				// handle the incoming msg
				switch splitWords[0] {

				case "SET":
					if len(splitWords) != 3 {
						fmt.Println("wrong SET command")
						fmt.Println(splitWords)
						continue
					}

					block := strings.Split(splitWords[1], ".")

					if len(block) != 2 {
						fmt.Println("block length not enough")
						continue
					}

					// Init release channel
					if _, ok := releaseChan[block[1]]; ok {
						releaseChan[block[1]][curIp] = make(chan bool)
					} else {
						tmp := make(map[string] chan bool)
						tmp[curIp] = make(chan bool)
						releaseChan[block[1]]= tmp
					}
					//fmt.Println("Init ok", curIp)
					//fmt.Println("Init ok", releaseChan)

					// search mutexMap4Ip to check if this IP has currently succeed in acquiring this lock
					flag := 0
					if _, ok := mutexMap4Ip[curIp]; ok {
						for key, val := range mutexMap4Ip[curIp] {
							if block[1] == key {
								if val == "R" {
									mutexMap4Ip[curIp][key] = "W"
									//fmt.Println("gaicheng w la")
								}
								handleSet(conn, block[1], splitWords[2])
								flag = 1
								break
							}
						}
					}

					fmt.Println("first set", mutexMap4Ip[curIp])
					// if not, go acquiring lock
					if flag == 0 {
						fmt.Println(curIp, "set acquiring lock...")

						mutexMapLock.Lock()
						if _, ok := mutexMap[block[1]]; !ok {
							//mutexMap[block[1]] = &sync.RWMutex{}
							mutexMap[block[1]] = "U"
						}
						//mutexMap[block[1]].Lock()
						if mutexMap[block[1]] == "U" {

							mutexMap[block[1]] = "W"
							mutexMapLock.Unlock()

							if _, ok := mutexMap4Ip[curIp]; ok {
								mutexMap4Ip[curIp][block[1]] = "W"
								// fmt.Println("set in", mutexMap4Ip[curIp][block[1]])
							} else {
								tmp := make(map[string]string)
								tmp[block[1]] = "W"
								mutexMap4Ip[curIp] = tmp
								// fmt.Println("set un", mutexMap4Ip[curIp][block[1]])
							}
							fmt.Println(curIp,"set acquiring successful...")
							fmt.Println(mutexMap4Ip[curIp])
							handleSet(conn, block[1], splitWords[2])

							// coordinator
							if coButton == "1" {
								msg := "SERVER||" + curIp + "||" + splitWords[1] + "||W\n"
								_, err := coConn.Write([]byte(msg))

								if err != nil {
									fmt.Println("Error to write coordinator msg ", err)
								}
							}

						} else {
							fmt.Println(mutexMap)
							mutexMapLock.Unlock()

							fmt.Println("Now we are at competence!")

							waitListLock.Lock()
							waitList[block[1]] = append(waitList[block[1]], curIp)
							waitListLock.Unlock()

							select {
							case <- abortChan[curIp]:

								waitListLock.Lock()
								waitList[block[1]] = removeElem(waitList[block[1]], curIp)
								waitListLock.Unlock()

								handleAbort(conn)

							case <- releaseChan[block[1]][curIp]:
								fmt.Println("released!", len(waitList[block[1]]), curIp)
								mutexMapLock.Lock()
								mutexMap[block[1]] = "W"
								mutexMapLock.Unlock()

								waitListLock.Lock()
								waitList[block[1]] = removeElem(waitList[block[1]], curIp)
								waitListLock.Unlock()
								fmt.Println("released!", len(waitList[block[1]]), curIp)

								fmt.Println(waitList[block[1]])

								if _, ok := mutexMap4Ip[curIp]; ok {
									mutexMap4Ip[curIp][block[1]] = "W"
									// fmt.Println("set in", mutexMap4Ip[curIp][block[1]])
								} else {
									tmp := make(map[string]string)
									tmp[block[1]] = "W"
									mutexMap4Ip[curIp] = tmp
									// fmt.Println("set un", mutexMap4Ip[curIp][block[1]])
								}
								fmt.Println(curIp,"set acquiring successful...")
								handleSet(conn, block[1], splitWords[2])

								// coordinator
								if coButton == "1" {
									msg := "SERVER||" + curIp + "||" + splitWords[1] + "||W\n"
									_, err := coConn.Write([]byte(msg))

									if err != nil {
										fmt.Println("Error to write coordinator msg ", err)
									}
								}
							}
						}
					}

				case "GET":
					if len(splitWords) != 2 {
						fmt.Println("wrong GET command")
						fmt.Println(splitWords)
						continue
					}

					block := strings.Split(splitWords[1], ".")
					if len(block) != 2 {
						fmt.Println("block length not enough")
						continue
					}

					// Init release channel
					if _, ok := releaseChan[block[1]]; ok {
						releaseChan[block[1]][curIp] = make(chan bool)
					} else {
						tmp := make(map[string] chan bool)
						tmp[curIp] = make(chan bool)
						releaseChan[block[1]]= tmp
					}
					//fmt.Println("Init ok", curIp)
					//fmt.Println("Init ok", releaseChan)

					// search mutexMap4Ip to check if this IP has currently succeed in acquiring this lock
					flag := 0
					if _, ok := mutexMap4Ip[curIp]; ok {
						for key := range mutexMap4Ip[curIp] {
							if block[1] == key {
								handleGet(conn, block[1])
								flag = 1
								break
							}
						}
					}

					// if not, go acquiring lock
					if flag == 0 {
						fmt.Println(curIp, "get acquiring lock...")

						mutexMapLock.Lock()
						if _, ok := mutexMap[block[1]]; !ok {
							//mutexMap[block[1]] = &sync.RWMutex{}
							mutexMap[block[1]] = "U"
						}
						//mutexMap[block[1]].Lock()
						if mutexMap[block[1]] == "U" {

							mutexMap[block[1]] = "R"
							mutexMapLock.Unlock()

							if _, ok := mutexMap4Ip[curIp]; ok {
								mutexMap4Ip[curIp][block[1]] = "R"
								// fmt.Println("set in", mutexMap4Ip[curIp][block[1]])
							} else {
								tmp := make(map[string]string)
								tmp[block[1]] = "R"
								mutexMap4Ip[curIp] = tmp
								// fmt.Println("set un", mutexMap4Ip[curIp][block[1]])
							}
							fmt.Println(curIp,"get acquiring successful...")
							handleGet(conn, block[1])

							// coordinator
							if coButton == "1" {
								msg := "SERVER||" + curIp + "||" + splitWords[1] + "||R\n"
								_, err := coConn.Write([]byte(msg))

								if err != nil {
									fmt.Println("Error to write coordinator msg ", err)
								}
							}

						} else {
							mutexMapLock.Unlock()

							fmt.Println("Now we are at competence!")

							waitListLock.Lock()
							waitList[block[1]] = append(waitList[block[1]], curIp)
							waitListLock.Unlock()

							select {
							case <- abortChan[curIp]:

								waitListLock.Lock()
								waitList[block[1]] = removeElem(waitList[block[1]], curIp)
								waitListLock.Unlock()

								handleAbort(conn)

							case <- releaseChan[block[1]][curIp]:

								mutexMapLock.Lock()
								mutexMap[block[1]] = "R"
								mutexMapLock.Unlock()

								waitListLock.Lock()
								waitList[block[1]] = removeElem(waitList[block[1]], curIp)
								waitListLock.Unlock()
								fmt.Println("get released!", len(waitList[block[1]]), curIp)

								fmt.Println("set in", mutexMap4Ip[curIp])

								if _, ok := mutexMap4Ip[curIp]; ok {
									mutexMap4Ip[curIp][block[1]] = "R"
									fmt.Println("set in", mutexMap4Ip[curIp])
								} else {
									tmp := make(map[string]string)
									tmp[block[1]] = "R"
									mutexMap4Ip[curIp] = tmp
									// fmt.Println("set un", mutexMap4Ip[curIp][block[1]])
								}
								fmt.Println(curIp,"get acquiring successful...")
								handleGet(conn, block[1])

								// coordinator
								if coButton == "1" {
									msg := "SERVER||" + curIp + "||" + splitWords[1] + "||R\n"
									_, err := coConn.Write([]byte(msg))

									if err != nil {
										fmt.Println("Error to write coordinator msg ", err)
									}
								}
							}
						}
					}

				// lock release & set undone trans to done trans
				case "COMMIT":
					handleCommit(conn)

				default:
					fmt.Println("Accept Wrong message, ignore")
				}
			}

		case rawStr := <- abortChan[conn.RemoteAddr().String()]:
			fmt.Println("abort!")
			if rawStr == true {
				handleAbort(conn)
			}
		}

	}
}

func handleSet(conn net.Conn, block string, balance string) {

	curIp := conn.RemoteAddr().String()
	atoiBalance, _ := strconv.Atoi(balance)

	if _, ok := undoneTrans[curIp]; ok {
		undoneTrans[curIp][block] = atoiBalance
	} else {
		tmp := make(map[string]int)
		tmp[block] = atoiBalance
		undoneTrans[curIp] = tmp
	}
	fmt.Println(undoneTrans)

	_, err := conn.Write([]byte("OK\n"))

	//fmt.Println("Write OK")

	checkError(err)
	return
}

func handleGet(conn net.Conn, block string) {
	curIp := conn.RemoteAddr().String()
	// ip -> {"x":10, "y":5}
	if _, ok := undoneTrans[curIp]; ok {
		if balance, ok := undoneTrans[curIp][block]; ok {
			balanceStr := strconv.Itoa(balance)
			msg := serverName + "." + block + " = " + balanceStr
			_, err := conn.Write([]byte(msg))
			checkError(err)
		} else {
			// if not found in undoneTrans, the go to doneTrans, block -> 3
			if balance, ok := doneTrans[block]; ok {
				msg := serverName + "." + block + " = " + balance
				_, err := conn.Write([]byte(msg))
				checkError(err)
			} else {
				_, err := conn.Write([]byte("NOT FOUND\n"))
				checkError(err)
				handleAbort(conn)
			}
		}
	// if dont have undone trans for this IP, then go to doneTrans
	} else {
		if balance, ok := doneTrans[block]; ok {
			msg := serverName + "." + block + " = " + balance
			_, err := conn.Write([]byte(msg))
			checkError(err)
		} else {
			_, err := conn.Write([]byte("NOT FOUND\n"))
			checkError(err)
			handleAbort(conn)
		}
	}
}

func handleCommit(conn net.Conn) {

	fmt.Println("Committing!")
	curIp := conn.RemoteAddr().String()

	if _, ok := mutexMap4Ip[curIp]; ok {
		for block := range mutexMap4Ip[curIp] {
			fmt.Println(mutexMap4Ip[curIp])
			mutexMapLock.Lock()
			mutexMap[block] = "U"
			mutexMapLock.Unlock()

			fmt.Println(block)

			if _, ok := waitList[block]; ok {
				if len(waitList[block]) > 0 {
					fmt.Println(waitList[block])
					if _, ok := releaseChan[block]; ok {
						//fmt.Println("lalalal")
						//fmt.Println(releaseChan[block])
						for _, ip := range waitList[block] {
							fmt.Println(ip)
							if _, ok := releaseChan[block][ip]; ok {
								// fmt.Println("akfj")
								releaseChan[block][ip] <- true
							}
							break
						}
					} else {
						tmp := make(map[string] chan bool)
						for _, ip := range waitList[block] {
							tmp[ip] = make(chan bool)
							releaseChan[block] = tmp
							releaseChan[block][ip] <- true
							break
						}
					}
					//fmt.Println("123124")
				}
			}
		}

		delete(mutexMap4Ip, curIp)
	}

	if _, ok := undoneTrans[curIp]; ok {
		for block, balance := range undoneTrans[curIp] {
			balanceStr := strconv.Itoa(balance)
			doneTrans[block] = balanceStr
		}

		delete(undoneTrans, curIp)
	}

	_, err := conn.Write([]byte("COMMIT OK\n"))
	checkError(err)
}

func handleAbort(conn net.Conn) {

	curIp := conn.RemoteAddr().String()

	if _, ok := mutexMap4Ip[curIp]; ok {
		for block := range mutexMap4Ip[curIp] {

			mutexMapLock.Lock()
			mutexMap[block] = "U"
			mutexMapLock.Unlock()

			if _, ok := waitList[block]; ok {
				if len(waitList[block]) > 0 {
					fmt.Println(waitList[block])
					if _, ok := releaseChan[block]; ok {
						for _, ip := range waitList[block] {
							fmt.Println(ip)
							if _, ok := releaseChan[block][ip]; ok {
								releaseChan[block][ip] <- true
							}
							break
						}
					} else {
						tmp := make(map[string] chan bool)
						for _, ip := range waitList[block] {
							tmp[ip] = make(chan bool)
							releaseChan[block] = tmp
							releaseChan[block][ip] <- true
							break
						}
					}
					// fmt.Println("123124")
				}
			}
		}

		delete(mutexMap4Ip, curIp)
	}

	if _, ok := undoneTrans[curIp]; ok {
		delete(undoneTrans, curIp)
	}

	_, err := conn.Write([]byte("ABORTED\n"))
	checkError(err)
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

func startCoordinator(conn *net.TCPConn) {
	for {
		buf := make([]byte, 1024)
		read_len, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error to read message because of ", err)
			break
		}

		readData := string(buf[0:read_len])
		fmt.Println("from coordinator", readData)

		conn.Write([]byte(readData))

		ip := strings.Split(readData, "||")[1]

		for key := range abortChan {
			label := strings.Split(key, ":")[0]
			if ip == label {
				abortChan[key] <- true
			}
		}
	}
}


func main() {

	// home : 10.182.191.160
	// school : 10.192.186.30
	// 10.192.186.30
	// var intro_serv_ip = "10.192.186.30:4444"

	if len(os.Args) != 4 {
		fmt.Println(os.Stderr, "Incorrect number of parameters, suppose to have server name")
		fmt.Println(len(os.Args))
		os.Exit(1)
	}

	globalInit()

	serverName = os.Args[1]
	portNum = os.Args[2]
	coButton = os.Args[3]
	localIp = getOwnIP()[0]

	if coButton == "1" {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", coordinator)
		checkError(err)
		coConn, err = net.DialTCP("tcp", nil, tcpAddr)
		checkError(err)
		go startCoordinator(coConn)
	}

	fmt.Println(getOwnIP()[0])

	startServer()

	// for {}
}