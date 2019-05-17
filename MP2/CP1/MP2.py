import socket
import threading
import time
import thread
import os
import random
import math
import sys

server_ip = "172.22.94.198" # you may change this server ip
server_port = 4444 # you may change this server port

if len(sys.argv) != 5:
    print("Usage: MP2.py <CONNECT> <node number> <ip> <port>") # Plz type python MP2.py CONNECT {node number} {ip} {port} to start
    sys.exit(1)

op = str(sys.argv[1])
nodeNum = str(sys.argv[2])
ip = str(sys.argv[3])
port = str(sys.argv[4])

service_bd = 0 # record bandwidth
socket_bd = 0 # record bandwidth

class Node:
    def __init__(self, host, nodeNum, ip, port):
        self.host = host
        self.nodeNum = nodeNum
        self.port = int(port)
        self.ip = ip
        self.connection = {} # conncetion[node1] = ['ip', 'port']
        self.transaction = {}
        self.lost = []
        self.query = " ".join(["QUERY", nodeNum, ip, port])+"\n"

    def gossip_trans(self, msg):
        '''
        Used for Gossip Trans
        '''
        count = 0
        neighbor_set = list(set(self.connection.keys()) - set(self.lost))
        if neighbor_set:
            loops = int(2 * math.log(len(neighbor_set), 2))
            while count <= loops:
                intro_nums = min(len(neighbor_set), 2)
                intros = random.sample(neighbor_set, intro_nums)
                for i in intros:
                    if i not in self.lost:
                        self.socket_send(i, self.connection[i][0], self.connection[i][1], msg)
                        time.sleep(0.1)
                count += 1
        return 0

    def socket_send(self, node, dest, port, msg): 
    	'''
    	 Method for client socket
    	'''
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        try:
            s1.connect((dest, int(port)))
        except socket.error, e:
            print (node + " is down!")
            print ('Strange error:%s' %e)
            self.lost.append(node)
            s1.close()
            return -1

        try:
            s1.sendall(msg.encode())
        except socket.error, e:
            print (node + " is down!")
            print ('Strange error:%s' %e)
            self.lost.append(node)
            s1.close()
            return -1

        s1.close()
        return 0

    def socket_rcv(self):
        '''
        Server for other nodes
        '''
        global socket_bd
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s2.bind((self.host, self.port))
        s2.listen(1000)
        while True:
            conn, addr = s2.accept()
            while True:
                rcv = conn.recv(1024)
                if not rcv:
                    break
                
                socket_bd += 1
                
                rcv = rcv.split("\n")

                for rcv_data in rcv:

                    rcv_data = rcv_data.split(" ")

                    if not rcv_data:
                        continue

                    if rcv_data[0] == "TRANSACTION":
                    	
                        if len(rcv_data) != 6:
                            print (rcv_data)
                            continue

                        if rcv_data[2] not in self.transaction:

                            self.transaction[rcv_data[2]] = " ".join(rcv_data)
                            mes = "From nodes: " + self.transaction[rcv_data[2]] + "-----" + str("%.6f" % time.time()) + "\n"
                            with open ('transaction_'+self.nodeNum+'.log', 'a') as f1:
                                f1.write(mes)
                            threading.Thread(target = self.gossip_trans, args = (self.transaction[rcv_data[2]], )).start()
                        	
                    if rcv_data[0] == "QUERY":
                        print (rcv_data)
                        if rcv_data[1] not in self.connection and rcv_data[1] != self.nodeNum:
                            self.connection[rcv_data[1]] = [rcv_data[2], rcv_data[3]]
                            mes = "Join: " + str(rcv_data[1]) + " Total: " + str(self.connection) + "-----" + str("%.6f" % time.time()) + "\n"
                            with open('connection_'+self.nodeNum+'.log', 'a') as f2:
                                f2.write(mes)
                        if rcv_data[1] not in self.lost:
                            threading.Thread(target = self.send_update, args = (rcv_data[1], rcv_data[2], rcv_data[3])).start()

                    if rcv_data[0] == "UPDATE":
                        print (rcv_data)
                        if rcv_data[1] not in self.connection and rcv_data[1] != self.nodeNum:
                            self.connection[rcv_data[1]] = [rcv_data[2], rcv_data[3]]
                            mes = "Join: " + str(rcv_data[1]) + " Total: " + str(self.connection) + "-----" + str("%.6f" % time.time()) + "\n"
                            with open('connection_'+self.nodeNum+'.log', 'a') as f3:
                                f3.write(mes)

    def service_rcv(self, s):
        '''
        Server for service node
        '''
        global service_bd
        while True:
            rcv = s.recv(1024)

            if not rcv:
                print ("service node down")
                break

            rcv = rcv.split("\n")
            
            service_bd += 1

            for rcv_data in rcv:

                rcv_data = rcv_data.split(" ")

                if not rcv_data:
                        continue

                if rcv_data[0] == "INTRODUCE":
                        print (rcv_data)
                        if rcv_data[1] in self.connection:
                            continue
                        else:
                            if rcv_data[1] != self.nodeNum:
                                self.connection[rcv_data[1]] = [rcv_data[2], rcv_data[3]]
                                mes = "Join: " + str(rcv_data[1]) + " Total: " + str(self.connection) + "-----" + str("%.6f" % time.time()) + "\n"
                                with open('connection_'+self.nodeNum+'.log', 'a') as f4:
                                    f4.write(mes)
                                self.socket_send(rcv_data[1], rcv_data[2], rcv_data[3], self.query) # send query

                if rcv_data[0] == "TRANSACTION":

                    if len(rcv_data) != 6:
                        print (rcv_data)
                        continue

                    if rcv_data[2] not in self.transaction:
                        
                        self.transaction[rcv_data[2]] = " ".join(rcv_data)
                        mes = "From service: " + self.transaction[rcv_data[2]] + "-----" + str("%.6f" % time.time()) + "\n"
                        with open ('transaction_'+self.nodeNum+'.log', 'a') as f5:
                            f5.write(mes)
                        threading.Thread(target = self.gossip_trans, args = (self.transaction[rcv_data[2]], )).start()

                if rcv_data[0] == "DIE" or rcv_data[0] == "QUIT":
                    mes = "DIE at " + str("%.6f" % time.time()) + "\n"
                    with open('connection_'+self.nodeNum+'.log', 'a') as f6:
                        f6.write(mes)
                    os._exit(0) # kill the process

    def query_neighbor(self):
        '''
        Discover Mechanism: query neighbor once a sec
        '''
        while True:
            time.sleep(1)
            if self.connection:
                  neighbor_set = set(self.connection.keys()) - set(self.lost)
                  if neighbor_set:
                    neighbor = (random.sample(list(neighbor_set), 1))[0]
                    self.socket_send(neighbor, self.connection[neighbor][0], self.connection[neighbor][1], self.query) # send query

    def send_update(self, node, dest, port):
        '''
        Send update Message to the queried node
        '''
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            s1.connect((dest, int(port)))
        except socket.error, e:
            print (node + " is down! UPDATE")
            print ('Strange error:%s' %e)
            self.lost.append(node)
            s1.close()
            return -1
        intros = random.sample(self.connection.keys(), len(self.connection.keys())/2)
        for i in intros:
            time.sleep(0.5)
            msg = " ".join(["UPDATE", i, self.connection[i][0], self.connection[i][1]])+"\n"
            try:
                s1.sendall(msg.encode())
            except socket.error, e:
            	print (node + " is down! UPDATE")
            	print ('Strange error:%s' %e)
                self.lost.append(node)
                s1.close()
                return -1
        return 0


if __name__ == '__main__':

    ''' 
    For original input 
    '''
    # sentence = raw_input()
    # op, nodeNum, ip, port = sentence.split(" ")

    sentence = " ".join([op, nodeNum, ip, port])
    host = socket.gethostname()
    node = Node(host, nodeNum, ip, port)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.connect((server_ip, int(server_port)))
    except:
        print ("service node is down")
        s.close()

    t1 = threading.Thread(target = node.query_neighbor)
    t2 = threading.Thread(target = node.socket_rcv)
    t3 = threading.Thread(target = node.service_rcv, args = (s,))

    t1.daemon = True
    t2.daemon = True
    t3.daemon = True

    mes = "Connected at: " + str("%.6f" % time.time()) + "\n"
    with open ('transaction_'+nodeNum+'.log', 'a') as f0:
        f0.write(mes)
    
    t2.start()
    t3.start()
    
    try:
        s.sendall((sentence+"\n").encode())
    except:
        print ("service node is down")
        s.close()
    
    t1.start()

    while True:
       start = time.time()
       time.sleep(1)
       with open('bandwidth_'+nodeNum+'.log', 'a') as f:
           duration = time.time()-start
           sens = str(float((socket_bd + service_bd)/duration))+"-----"+str(duration)+"\n"
           f.write(sens)
       socket_bd = 0
       service_bd = 0

