#This is the first MP of UIUC CS425. Download the file and upload it on the the virtual mathine.
#Our Group number is 57. Just type :  Python MP1.py name port n
#Then the program will wait until all other machine joinning the chat room.
#Anyone press /q, he will leave the chat room.
#Anyone lost connection, others's will show that 'name has failed'.
#Make sure the port you type in is not 9999. Because we use 9999 to receive the heartbeats.

import socket
import threading
import time
import thread
import sys


class Node:
    def __init__(self, host, name, port,n,heartbeat_port,interval):
        self.host = host    #host name
        self.port = int(port)  #port for message
        self.heartbeat_port = int(heartbeat_port) #port for heartbeat
        self.name = name
        self.interval = interval  #interval between every heartbeat
        self.n = n 
        self.connection = ["sp19-cs425-g57-01.cs.illinois.edu", "sp19-cs425-g57-02.cs.illinois.edu", 
                           "sp19-cs425-g57-03.cs.illinois.edu", "sp19-cs425-g57-04.cs.illinois.edu",
                           "sp19-cs425-g57-05.cs.illinois.edu", "sp19-cs425-g57-06.cs.illinois.edu",
                           "sp19-cs425-g57-07.cs.illinois.edu", "sp19-cs425-g57-08.cs.illinois.edu",
                           "sp19-cs425-g57-09.cs.illinois.edu", "sp19-cs425-g57-010.cs.illinois.edu",]
        
        self.stamp = [0] * 10
        self.index = self.host.split('-')[3] # depend on vm
        self.index = int(self.index.split('.')[0])
        self.buf = {}  # dictionary for time stamps and message
        self.time = {} # dictionary to store the last heartbeat time
        #--------------------reliable delivery------------------------------------
        self.received = []
        
#-------------------------Pack the Message-------------------------------
    def send_msg(self): # method for take input msg
        while True:
            usr_in = raw_input()
            if usr_in == "/q":
                msg = str(self.name)+"#%*/q"
                for i in self.connection:
                    self.socket_send(i, self.port, msg)
                sys.exit(0)

            self.stamp[self.index] += 1
            msg = str(self.name)+":"+str(usr_in)+"#%*"+".".join(str(v) for v in self.stamp)
            msg_self = str(self.name)+":"+str(usr_in)
            print(msg_self)
            # print(self.stamp)
            for i in self.connection:
                self.socket_send(i, self.port, msg)
            # self.stamp[self.index] -= 1

#-------------------------Sending Message-------------------------------
    def socket_send(self, host, port, msg): #method for multicasting
    	'''
    	 method for client socket
    	'''
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            s1.connect((host, port))
        except:
            # print(s1)
            # self.connection.remove(host)
            # print(self.connection)
            # print (host + " Not Online")
            s1.close()
            return -1
        try:
            s1.sendall(msg)
        except:
            s1.close()
            return -1

        s1.close()
        return 0
#-------------------------Receiving Message-------------------------------
    def socket_rcv(self):  
        # print type(self.port)
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.bind((self.host, self.port))
        s2.listen(10)
        while True:
            conn, addr = s2.accept()
            # print 'Connected by ', addr
            while True:
                rcv_data = conn.recv(1024)
                
                if not rcv_data: # recv ending msg from client
                    break
                
                if rcv_data not in self.received:

                    self.received.append(rcv_data)

                    self.send_reliable(rcv_data)

                    tmp_data, tmp_stamp = rcv_data.split("#%*")
                    if tmp_stamp =="/q":     # If the input is /q, it means he/she will quit the room
                        print(tmp_data+" has left")
                    if tmp_data == self.name:
                        sys.exit(0)
                    if tmp_stamp =="/q":
                        continue

                    self.buf[tmp_stamp] = tmp_data
                    if (addr != socket.gethostname()):
                        self.causal_ordering(self.buf.keys())

            # self.causal_ordering(self.buf.keys())

            conn.close() # close

#-------------------------Causal Ording---------------------------------    
    def causal_ordering(self, buf_keys):  
        for i in buf_keys:
            outer_stamp = i.split(".")
            outer_stamp = list(map(int, outer_stamp))
            flag = 0
            count = 0
            index = 0
            for j in range(len(outer_stamp)):
                if (outer_stamp[j] <= self.stamp[j]):
                    continue
                elif (outer_stamp[j] == self.stamp[j] + 1):
                    count += 1
                    index = j
                else:
                    flag = 1
            if count == 1 and flag == 0:
                print (self.buf[i])
                del self.buf[i]
                self.stamp[index] += 1
                # print(self.stamp)
    
#-------------------------Send Heartbeat-------------------------------
    def send_heartbeat(self): # method for take input msg
        pre_time = time.time()*1000
        while True:
            time.sleep(self.interval/10000)
            curr_time = time.time()*1000
            if (curr_time - pre_time)>self.interval:
                pre_time = time.time()
                msg = self.name
                for i in self.connection:
                    self.socket_send(i, self.heartbeat_port, msg)

#-------------------------Receiving Heartbeat-------------------------------    
    def rcv_heartbeat(self):
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.bind((self.host, self.heartbeat_port))
        s2.listen(10)
        connected_num = 0
        while True:
            for i in self.time.keys():
                now = time.time()*1000
                if now - self.time[i] > 3.5*self.interval:
                    print(i+" has failed")
                    del self.time[i]
            conn, addr = s2.accept()
            while True:
                rcv_data = conn.recv(1024)
                if not rcv_data: # recv ending msg from client
                    break
                if rcv_data not in self.time.keys():
                    connected_num+=1
                if connected_num == int(self.n):
                    print("Ready")
                    connected_num+=1
                tmp_time = time.time()
                self.time[rcv_data] = tmp_time*1000
            conn.close() # close

#-------------------------Reliable Multicast-------------------------------  
    # socket_send(self, host, port, msg)
    def send_reliable(self, msg):
        for i in self.connection:
            self.socket_send(i, self.port, msg)



#-------------------------Main Function-------------------------------
if __name__ == '__main__':
    sentence = sys.argv
    sentence_len = len(sentence)
    # print(sentence_len)
    # print(sentence)
    if sentence_len != 4:
        print("Wrong input, input should be: name  port  number")
        sys.exit(0)
    name = sentence[1]
    port = sentence[2]
    n = sentence[3]
    heartbeat_port = 9999
    interval = 4000

    host = socket.gethostname()
    # print(host)
    node = Node(host, name, port, n, heartbeat_port, interval)
    t1 = threading.Thread(target = node.send_msg)
    t2 = threading.Thread(target = node.socket_rcv)
    t3 = threading.Thread(target = node.send_heartbeat)
    t4 = threading.Thread(target = node.rcv_heartbeat)

    t1.daemon = True
    t2.daemon = True
    t3.daemon = True
    t4.daemon = True

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    while True:
        pass


