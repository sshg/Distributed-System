package main

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	node_name string
	local_ip_address string
	port_number string

	trans_num int = 30

	block_id int = 1
	block_id_mutex = sync.RWMutex{}
	block_hash string = "0000000000000000000000000000000000000000000000000000000000000000"

	init_stat bool = true

	// bd_count int = 0
	// bd_mutex = sync.RWMutex{}
)

var (
	client_read_chan chan string
	client_write_chan chan string
	stop_chan chan bool
	server_read_chan chan string
	// server_write_chan chan string

	gossip_block_chan chan string
	gossip_trans_chan chan string
)

var (
	conn_list map[string]*net.TCPConn
	conn_list_detail map[string] []string
	conn_list_mutex = sync.RWMutex{}
	conn_list_detail_mutex = sync.RWMutex{}

	committed_trans_list map[string] []string // key = trans id, value = transaction
	committed_trans_list_mutex = sync.RWMutex{}

	service_trans_list []transaction // trans sent by service
	service_trans_list_mutex = sync.RWMutex{}

	trans_waitlist map[string] []transaction // key = hash, value = []trans
	trans_waitlist_mutex = sync.RWMutex{}

	block_list map[string]block // key = block hash, value = struct block, committed block list
	block_list_mutex = sync.RWMutex{}

	out_block_list map[string] block // new blocks from outside, string = hash, value = block
	out_block_list_mutex = sync.RWMutex{}

	account_list []int // balance
	account_list_mutex = sync.RWMutex{}

	tentative_block_list map[string] block // generated block tentative to be committed, key = block hash, value = block

	// bd_list []string // used to record bandwidth
)

type transaction struct {
	timestamp string
	id string
	src_account string
	dest_account string
	amount string
	hash string
	solution string
	fromwhom string
}

type block struct {
	id int
	hash string
	solution string
	appearTime string
	rcvTime string
}

func globalInit() {
	client_read_chan = make(chan string)
	client_write_chan = make(chan string)
	server_read_chan = make(chan string)
	// server_write_chan = make(chan string)

	stop_chan = make(chan bool)
	gossip_block_chan = make(chan string)
	gossip_trans_chan = make(chan string)

	conn_list = make(map[string]*net.TCPConn)
	conn_list_detail = make(map[string][]string)
	conn_list_detail[node_name] = []string{local_ip_address, port_number}

	trans_waitlist = make(map[string] []transaction)
	block_list = make(map[string] block)
	out_block_list = make(map[string] block)
	tentative_block_list = make(map[string] block)
	committed_trans_list = make(map[string] []string)
	account_list = make([]int, 2000)

	// bd_list = make([]string, 300)
}

// func bandwidth() {
// 	duration, _ := time.ParseDuration("1000ms")
// 	time.Sleep(duration)
// 	bd_mutex.Lock()
// 	tmp := strconv.Itoa(bd_count)
// 	bd_list = append(bd_list, tmp)
// 	bd_count = 0
// 	bd_mutex.Unlock()
// }

func gossip_trans() {
	for {
		select {

		case gossipBlock := <- gossip_block_chan:

			if len(conn_list) == 0 {
				duration, _ := time.ParseDuration("200ms")
				time.Sleep(duration)
				continue
			}

			conn_list_mutex.RLock()
			for _, conn := range conn_list {
				conn_list_mutex.RUnlock()
				conn.Write([]byte(gossipBlock))
				conn_list_mutex.RLock()
			}
			conn_list_mutex.RUnlock()


		case gossipData := <- gossip_trans_chan:

			if len(conn_list) == 0 {
				duration, _ := time.ParseDuration("200ms")
				time.Sleep(duration)
				continue
			}


			conn_list_mutex.RLock()
			for _, conn := range conn_list {
				conn_list_mutex.RUnlock()
				conn.Write([]byte(gossipData))
				conn_list_mutex.RLock()
			}
			conn_list_mutex.RUnlock()
		}

	}
}

func startServer() {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", local_ip_address + ":"+ port_number)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()  // close connection before exit
	// conn.SetReadDeadline(time.Now().Add(2 * time.Minute)) // set 2 minutes timeout

	go serverRead(conn)
	//go serverWrite(conn)

	for {
		select {

		case rawStr := <- server_read_chan:

			// bd_mutex.Lock()
			// bd_count += 1
			// bd_mutex.Unlock()

			rawIn := strings.Split(rawStr, "\n")

			for _, readStr := range(rawIn) {

				splitWords := strings.Fields(readStr)

				if len(splitWords) == 0 {
					continue
				}

				if splitWords[0] == "INTRODUCE" {

					if len(splitWords) != 4 {
						continue
					}

					// check if it's yourself
					if splitWords[1] == node_name {
						continue
					}

					// add node to list and send back intro
					handleIntro(splitWords[1], splitWords[2], splitWords[3])
				}

				if splitWords[0] == "TRANSACTION" { // "TRANSACTION TIME ID SRC DEST AMOUNT BLOCK_HASH BLOCK_SOLUTION"

					if len(splitWords) != 8 {
						fmt.Println("gossip transaction rcv not enough length\n")
						fmt.Println(splitWords)
						continue
					}

					if _, ok := committed_trans_list[splitWords[2]]; ok {
						// fmt.Println("exist in committed_trans_list\n")
						continue
					}

					// fmt.Println(readStr)

					committed_trans_list_mutex.Lock()
					timestamp := time.Now().UnixNano()/1e3
					timeS := strconv.FormatInt(timestamp, 10)
					info := append(splitWords, timeS)
					committed_trans_list[splitWords[2]] = info
					committed_trans_list_mutex.Unlock()

					// gossip rcv trans
					gossip_trans_chan <- readStr + "\n"

					// file write
					//_, file_err = transLog.WriteString("From node: " + readStr)
					//checkError(file_err)

					fmt.Println("From node: " + readStr)

					// since the transaction rcv is valid, no need to check account
					src_acc, _ := strconv.Atoi(splitWords[3])
					dest_acc, _ := strconv.Atoi(splitWords[4])
					amo, _ := strconv.Atoi(splitWords[5])

					if src_acc == 0 {
						account_list[dest_acc] += amo
						continue
					} else {
						account_list[src_acc] -= amo
						account_list[dest_acc] += amo
					}

				}

				if splitWords[0] == "BLOCK" { // "BLOCK ID HASH SOLUTION appearTime"

					// check if valid
					if len(splitWords) != 5 {
						continue
					}

					// check if the block has been committed
					block_list_mutex.RLock()
					if _, ok := block_list[splitWords[2]]; ok {
						block_list_mutex.RUnlock()
						continue
					}
					block_list_mutex.RUnlock()

					out_block_list_mutex.RLock()
					// check if the block has been rcv
					if _, ok := out_block_list[splitWords[2]]; ok {
						out_block_list_mutex.RUnlock()
						continue
					}
					out_block_list_mutex.RUnlock()

					// put block info to out block list
					out_block_list_mutex.Lock()
					out_block_id, _ := strconv.Atoi(splitWords[1])
					out_block_list[splitWords[2]] = block{id: out_block_id, hash: splitWords[2], solution: splitWords[3], appearTime:splitWords[4]}

					if out_block_id >= block_id {
						verify_msg := "VERIFY " + splitWords[2] + " " + splitWords[3] + "\n"
						client_write_chan <- verify_msg
					} else {
						fmt.Println("drop outer block ", splitWords[2], " with id ", out_block_list[splitWords[2]].id,
							" current id: ", block_id)
					}
					out_block_list_mutex.Unlock()
				}
			}
		}
	}
}

func startClient(tcpaddr string, msg []byte) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", tcpaddr)
	checkError(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	conn.Write(msg)

	defer conn.Close()

	go clientRead(conn)
	go clientWrite(conn)

	for {
		select {

		case rawIn := <-client_read_chan:

			// bd_mutex.Lock()
			// bd_count += 1
			// bd_mutex.Unlock()

			rawStr := strings.Split(rawIn, "\n")

			for _, readStr := range rawStr {

				splitWords := strings.Fields(readStr)

				if len(splitWords) == 0 {
					continue
				}

				if splitWords[0] == "DIE" || splitWords[0] == "QUIT" {
					stop_chan <- true
				}

				if splitWords[0] == "INTRODUCE" {

					if len(splitWords) != 4 {
						continue
					}

					handleIntro(splitWords[1], splitWords[2], splitWords[3])
				}

				if splitWords[0] == "VERIFY" {
					if splitWords[1] == "OK" {

						// add new block to block list
						timestamp := time.Now().UnixNano()/1e3
						timeS := strconv.FormatInt(timestamp, 10)

						block_list_mutex.Lock()
						out_block_list_mutex.RLock()
						block_list[splitWords[2]] = block{id: out_block_list[splitWords[2]].id,
							hash: splitWords[2], solution: splitWords[3], appearTime:out_block_list[splitWords[2]].appearTime, rcvTime:timeS}
						out_block_list_mutex.RUnlock()
						block_list_mutex.Unlock()

						// gossip rcv blocks
						block_list_mutex.RLock()
						tmpId := strconv.Itoa(block_list[splitWords[2]].id)
						block_msg := "BLOCK " + tmpId + " " + splitWords[2] + " " + splitWords[3] + " " + block_list[splitWords[2]].appearTime + "\n"
						block_list_mutex.RUnlock()
						gossip_block_chan <- block_msg

						// add block id
						block_id_mutex.Lock()
						block_id = out_block_list[splitWords[2]].id + 1
						fmt.Println("block id changed by outer block", block_id)
						block_id_mutex.Unlock()

						// generate new block hash
						block_hash = geneNewhash(splitWords[2], splitWords[3])

						// clear service_trans_list
						service_trans_list_mutex.RLock()
						trans_waitlist_mutex.Lock()
						trans_waitlist[block_hash] = service_trans_list
						trans_waitlist_mutex.Unlock()
						service_trans_list_mutex.RUnlock()

						service_trans_list_mutex.Lock()
						service_trans_list = service_trans_list[:0]
						service_trans_list_mutex.Unlock()

						// send new solve
						new_block_msg := "SOLVE " + block_hash + "\n"
						fmt.Println(new_block_msg)
						client_write_chan <- new_block_msg

						// generate tentative block
						tentative_block_list[block_hash] = block{id:block_id, hash:block_hash}

						// file write
						//tmpId := strconv.Itoa(out_block_list[splitWords[2]].id)
						//block_msg := "BLOCK " + tmpId + " " + splitWords[2] + " " + splitWords[3] + "\n"

						//_, err = transLog.WriteString("From node:" + block_msg)
						//checkError(err)

					} else if splitWords[1] == "FAIL" {
						fmt.Println("Verify Error")
						continue
					}
				}

				if splitWords[0] == "SOLVED" { // "SOLVED HASH SOLUTION"

					if tentative_block_list[splitWords[1]].id >= block_id {

						cur_splitWords := splitWords

						fmt.Println(splitWords)
						// add block id
						block_id_mutex.Lock()
						block_id += 1
						block_id_mutex.Unlock()

						// geneNewHash
						block_hash = geneNewhash(splitWords[1], splitWords[2])

						// clear service transactions
						service_trans_list_mutex.RLock()
						trans_waitlist_mutex.Lock()
						trans_waitlist[block_hash] = service_trans_list
						trans_waitlist_mutex.Unlock()
						service_trans_list_mutex.RUnlock()

						service_trans_list_mutex.Lock()
						service_trans_list = service_trans_list[:0]
						service_trans_list_mutex.Unlock()

						// add new block to block list
						timestamp := time.Now().UnixNano()/1e3
						timeS := strconv.FormatInt(timestamp, 10)

						block_list_mutex.Lock()
						block_list[splitWords[1]] = block{id: tentative_block_list[splitWords[1]].id,
							hash: splitWords[1], solution: splitWords[2], appearTime:timeS, rcvTime: timeS}
						block_list_mutex.Unlock()

						// gossip new block msg to all other nodes
						// "BLOCK ID HASH SOLUTION"
						tmpId := strconv.Itoa(tentative_block_list[splitWords[1]].id)

						block_list_mutex.RLock()
						block_msg := "BLOCK " + tmpId + " " + splitWords[1] + " " + splitWords[2] + " " + block_list[splitWords[1]].appearTime + "\n"
						block_list_mutex.RUnlock()

						fmt.Println(block_msg)
						gossip_block_chan <- block_msg

						// send new solve
						new_block_msg := "SOLVE " + block_hash + "\n"
						fmt.Println(new_block_msg)
						client_write_chan <- new_block_msg

						// generate tentative block
						tentative_block_list[block_hash] = block{id:block_id, hash:block_hash}

						// file write
						//_, err = transLog.WriteString(block_msg)
						//checkError(err)

						// gossip trans_waitlist to all other nodes
						//go func() {
						//	for _, trans := range trans_waitlist[cur_splitWords[1]] {
						//
						//		duration, _ := time.ParseDuration("5ms")
						//		time.Sleep(duration)
						//
						//		committed_trans_list_mutex.RLock()
						//		if _, ok := committed_trans_list[trans.id]; ok {
						//			committed_trans_list_mutex.RUnlock()
						//			continue
						//		}
						//		committed_trans_list_mutex.RUnlock()
						//
						//		// check transactions and commit to log file
						//		if checkAccount(trans.src_account, trans.dest_account, trans.amount) == true {
						//
						//			gossip_msg := "TRANSACTION " + trans.timestamp + " " + trans.id + " " +
						//				trans.src_account + " " + trans.dest_account + " " + trans.amount + " " +
						//				cur_splitWords[1] + " " + cur_splitWords[2] +  "\n"
						//
						//			gossip_trans_chan <- gossip_msg
						//
						//			//_, err = transLog.WriteString("From self: " + gossip_msg)
						//			//checkError(err)
						//
						//			fmt.Println("From self: " + gossip_msg)
						//
						//			timestamp := time.Now().UnixNano()/1e3
						//			timeS := strconv.FormatInt(timestamp, 10)
						//			info := strings.Fields(gossip_msg)
						//			info = append(info, timeS)
						//
						//			committed_trans_list_mutex.Lock()
						//			committed_trans_list[trans.id] = info
						//			committed_trans_list_mutex.Unlock()
						//
						//			continue
						//		}
						//	}
						//}()
						go gossipTrans(cur_splitWords)

					} else {
						fmt.Println("your own block isn't going to be committed due to other block's committed")
					}
				}


				if splitWords[0] == "TRANSACTION" {

					if len(splitWords) != 6 {
						fmt.Println("not enough len")
						continue
					}

					// generate transaction struct and put it into service trans list
					trans := transaction{timestamp:splitWords[1], id:splitWords[2], src_account:splitWords[3],
						dest_account:splitWords[4], amount:splitWords[5]}

					service_trans_list_mutex.Lock()
					service_trans_list = append(service_trans_list, trans)

					// check if init trans has reached number
					if len(service_trans_list) >= trans_num && init_stat {
						// generate tentative block
						tentative_block_list[block_hash] = block{id:block_id, hash:block_hash}

						// send solve
						new_block_msg := "SOLVE " + block_hash + "\n"
						fmt.Println(new_block_msg)
						client_write_chan <- new_block_msg

						// put accpeted trans into waitlist map (block hash as key)
						trans_waitlist_mutex.Lock()
						trans_waitlist[block_hash] = service_trans_list
						trans_waitlist_mutex.Unlock()

						// clear accepted trans buf
						service_trans_list = service_trans_list[:0]

						// shut down init
						init_stat = false
					}
					service_trans_list_mutex.Unlock()
				}
			}
		}
	}
}

func gossipTrans(cur_splitWords []string) {
	for _, trans := range trans_waitlist[cur_splitWords[1]] {

		duration, _ := time.ParseDuration("5ms")
		time.Sleep(duration)

		committed_trans_list_mutex.RLock()
		if _, ok := committed_trans_list[trans.id]; ok {
			committed_trans_list_mutex.RUnlock()
			continue
		}
		committed_trans_list_mutex.RUnlock()

		// check transactions and commit to log file
		if checkAccount(trans.src_account, trans.dest_account, trans.amount) == true {

			gossip_msg := "TRANSACTION " + trans.timestamp + " " + trans.id + " " +
				trans.src_account + " " + trans.dest_account + " " + trans.amount + " " +
				cur_splitWords[1] + " " + cur_splitWords[2] +  "\n"

			gossip_trans_chan <- gossip_msg

			//_, err = transLog.WriteString("From self: " + gossip_msg)
			//checkError(err)

			fmt.Println("From self: " + gossip_msg)

			timestamp := time.Now().UnixNano()/1e3
			timeS := strconv.FormatInt(timestamp, 10)
			info := strings.Fields(gossip_msg)
			info = append(info, timeS)

			committed_trans_list_mutex.Lock()
			committed_trans_list[trans.id] = info
			committed_trans_list_mutex.Unlock()
			continue
		}
	}
}

func checkAccount(src string, dest string, amount string) bool {
	src_acc, _ := strconv.Atoi(src)
	dest_acc, _ := strconv.Atoi(dest)
	amo, _ := strconv.Atoi(amount)

	account_list_mutex.Lock()
	if src_acc == 0 {
		account_list[dest_acc] += amo
		account_list_mutex.Unlock()
		return true
	}

	if account_list[src_acc] - amo < 0 {
		account_list_mutex.Unlock()
		return false
	}

	account_list[src_acc] -= amo
	account_list[dest_acc] += amo
	account_list_mutex.Unlock()
	return true

}

func geneNewhash(hash string, solution string) string {

	hash1, _ := hex.DecodeString(hash)
	solution1, _ := hex.DecodeString(solution)
	i := 0
	var tmp []byte
	var length int
	if len(hash1) <= len(solution1) {
		length = len(hash1)
	} else {
		length = len(solution1)
	}
	for i < length {
		tmp = append(tmp, hash1[i] - solution1[i])
		i += 1
	}

	h := sha256.New()
	h.Write(tmp)
	bs := hex.EncodeToString(h.Sum(nil))

	return bs
}

func handleIntro(nodeNum string, ip string, port string){

	conn_list_mutex.RLock()
	if _, ok := conn_list[nodeNum]; ok {
		conn_list_mutex.RUnlock()
		return
	}
	conn_list_mutex.RUnlock()

	tcpaddr := ip + ":" + port

	tcpAddr, err := net.ResolveTCPAddr("tcp4", tcpaddr)
	checkError(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	conn_list_mutex.Lock()
	conn_list[nodeNum] = conn
	conn_list_mutex.Unlock()

	conn_list_detail_mutex.Lock()
	conn_list_detail[nodeNum] = append(conn_list_detail[nodeNum], ip)
	conn_list_detail[nodeNum] = append(conn_list_detail[nodeNum], port)
	conn_list_detail_mutex.Unlock()

	//msg := "INTRODUCE " + node_name + " " + local_ip_address + " " + port_number + "\n"
	//server_write_chan <- msg

}

func clientRead(conn *net.TCPConn) {
	for {
		buf := make([]byte, 1024)
		read_len, err := conn.Read(buf)

		if err != nil {
			fmt.Println("client Error to read message because of ", err)
			break
		}

		readData := string(buf[0:read_len])

		client_read_chan <- readData
	}

	stop_chan <- true
}

func serverRead(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		read_len, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Server Error to read message because of ", err)
			break
		}

		readData := string(buf[0:read_len])

		server_read_chan <- readData
	}
}

func clientWrite(conn *net.TCPConn) {
	for {
		writeData := <- client_write_chan
		_, err := conn.Write([]byte(writeData))

		if err != nil {
			fmt.Println("Client Error to write message because of ", err)
			break
		}

	}
	stop_chan <- true
}

//func serverWrite(conn net.Conn) {
//	for {
//		writeData := <- server_write_chan
//		_, err := conn.Write([]byte(writeData))
//
//		if err != nil {
//			fmt.Println("Server Error to write message because of ", err)
//			break
//		}
//
//	}
//	stop_chan <- true
//}

func period_query_neighbor() {

	for {
		duration, _ := time.ParseDuration("250ms")
		time.Sleep(duration)
		// fmt.Println(conn_list_detail)
		conn_list_mutex.RLock()
		if len(conn_list) != 0 {
			for key, conn := range conn_list {
				conn_list_mutex.RUnlock()
				conn_list_detail_mutex.RLock()
				for key1, node := range conn_list_detail {
					conn_list_detail_mutex.RUnlock()
					if key1 != key{
						duration2, _ := time.ParseDuration("5ms")
						time.Sleep(duration2)
						msg := "INTRODUCE " + key1 + " " + node[0] + " " + node[1] + "\n"
						conn.Write([]byte(msg))
					}
					conn_list_detail_mutex.RLock()
				}
				conn_list_detail_mutex.RUnlock()
				break
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

	// home : 10.182.190.120
	// school : 10.192.186.30
	// 10.192.186.30
	// 10.182.188.148
	var intro_serv_ip = "172.22.94.198:4444"

	if len(os.Args) != 4 {
		fmt.Println(os.Stderr, "Incorrect number of parameters")
		fmt.Println(len(os.Args))
		os.Exit(1)
	}


	node_name = os.Args[1]
	local_ip_address = os.Args[2]
	port_number = os.Args[3]

	connect_message := "CONNECT " + node_name + " " + local_ip_address + " " + port_number + "\n"
	connect_message_byte := []byte(connect_message)

	transLogName := node_name + "_Trans.csv"
	connLogName := node_name + "_Conn.csv"
	// bandwidthName := node_name + "_Bandwidth.csv"

	transLog, file_err := os.Create(transLogName)
	checkError(file_err)
	connLog, file_err := os.Create(connLogName)
	checkError(file_err)
	// bdLog, file_err := os.Create(bandwidthName)

	transWriter := csv.NewWriter(transLog)
	transWriter.Flush()

	connWrite := csv.NewWriter(connLog)
	connWrite.Flush()

	// bdWriter := csv.NewWriter(bdLog)
	// bdWriter.Flush()

	globalInit()

	go startClient(intro_serv_ip, connect_message_byte)
	go gossip_trans()
	go period_query_neighbor()
	go startServer()
	// go bandwidth()
	<- stop_chan
	time.Sleep(3*time.Second)

	transWriter_mutex := sync.Mutex{}
	transWriter_mutex.Lock()
	for _, block := range block_list{
		tmpId := strconv.Itoa(block.id)
		transWriter.Write([]string{"BLOCK", tmpId, block.hash, block.solution, block.appearTime, block.rcvTime})
	}
	transWriter.Flush()
	transWriter_mutex.Unlock()

	transWriter_mutex.Lock()
	for _, value := range committed_trans_list{
		transWriter.Write(value)
	}
	transWriter.Flush()
	transWriter_mutex.Unlock()

	for _, connValue := range conn_list_detail {
		connWrite.Write(connValue)
	}
	connWrite.Flush()

	// if node_name == "node1" || node_name == "node5" {
	// 	for _, bdvalue := range bd_list {
	// 		bdWriter.Write([]string{bdvalue, "0"})
	// 	}
	// 	bdWriter.Flush()
	// }

	fmt.Println("finished")

	// for {}
}