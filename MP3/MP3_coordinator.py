import socket
import threading
import time
# import thread
import sys
# import numpy as np

class Coordinator:
    def __init__(self,host, port):
        self.host = host
        self.port = int(port)  #port for message
        self.abortList = []
        self.serverLock = {}
        self.clinentLock = {}
        self.source_ip = {}
        self.waitingLock = {}
        self.ip_wait_ip = {}
        self.serverAddr = {}
        
        
#-------------------------Sending Message-------------------------------
    def send_msg(self,abort_ip): # method for take input msg
            msg = "ABORT"
            for i in self.abortList:
                msg = msg+"||"+abort_ip

                # self.socket_send(i, self.port, msg)
                i.sendall(msg.encode())
            self.abortList.clear()

    def socket_send(self, host, port, msg): #method for multicasting
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
        # # print type(self.port)
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.bind((self.host, self.port))
        s2.listen(10)
        while True:
            conn, addr = s2.accept()
            # addr = "1111.3333.2222.1111"
            c = threading.Thread(target = self.single_sock, args = (conn, ))
            c.start()
            # conn.close() # close
            # print("abort list is: ",self.abortList,"\n" )
            # print("severLock list is: ",self.serverLock,"\n" )
            # print("ClinentLock list is: ",self.clinentLock,"\n" )
            # print("source_ip list is: ",self.source_ip,"\n" )
            # print("waitingLock list is: ",self.waitingLock,"\n" )
            # print("ip_wait_ip list is: ",self.ip_wait_ip,"\n" )
            # self.serverLock = {}
            # self.clinentLock = {}
            # self.source_ip = {}
            # self.waitingLock = {}
            # self.ip_wait_ip = {}

    def single_sock(self, conn):
        while True:
            rcv_data = str(conn.recv(1024)).split('\'')[1]

            print(rcv_data)
            if not rcv_data: # recv ending msg from client
                break
            #rcv_data: Server||ip||A.x||R    rcv_data: Clinent||ip||B.y||S
            data = rcv_data.split("||")   #rcv_data
            # both self.serverLock and self.clinent.Lock should be {ip:[A.x_R,B.y_W]}
            ip = data[1].split(':')[0]
            ip_now = ip
            if data[0] == "SERVER":

                if ip in self.serverAddr:
                    if conn not in self.serverAddr[ip]:
                        self.serverAddr[ip].append(conn)
                else:
                    self.serverAddr[ip] = [conn]

                val = "-".join(data[2:])
                if ip not in self.serverLock:
                    self.serverLock[ip] = [val]
                else:
                    self.serverLock[ip].append(val)
                
                if data[2] not in self.source_ip:
                    self.source_ip[data[2]] = [ip]
                else:
                    if val not in self.source_ip[data[2]]:
                        self.source_ip[data[2]].append(ip)
                
            elif data[0] == "CLIENT":  
                val = "-".join(data[2:])   
                if ip not in self.clinentLock:
                    self.clinentLock[ip] = [val]
                else:
                    if val not in self.clinentLock[ip]:
                        self.clinentLock[ip].append(val)
                print("ClinentLock list is: ",self.clinentLock,"\n" )

            elif data[0] == "COMMIT":    #Case of commit
                if ip in self.serverLock:
                    del self.serverLock[ip]
                if ip in self.clinentLock:
                    del self.clinentLock[ip]
                if ip in self.waitingLock:
                    del self.waitingLock[ip]
                if ip in self.ip_wait_ip:
                    del self.ip_wait_ip[ip]
                for key in self.source_ip:   #self.source_ip:  {A.x:[ip1,ip2....],B.y:[]...}
                    if ip not in self.source_ip[key]:
                        continue
                    else:
                        self.source_ip[key].remove(ip)
            
            elif data[0] == "ABORT":
                if ip in self.serverLock:
                    del self.serverLock[ip]
                if ip in self.clinentLock:
                    del self.clinentLock[ip]
                if ip in self.waitingLock:
                    del self.waitingLock[ip]
                if ip in self.ip_wait_ip:
                    del self.ip_wait_ip[ip]
                for key in self.source_ip:   #self.source_ip:  {A.x:[ip1,ip2....],B.y:[]...}
                    if ip not in self.source_ip[key]:
                        continue
                    else:
                        self.source_ip[key].remove(ip)
            else:
                continue


            # List the lock that every ip is waiting for
            for ip in self.clinentLock:
                tmp_waitingLock = self.clinentLock[ip]
                if ip not in self.serverLock:
                    self.waitingLock[ip] = tmp_waitingLock
                else:
                    for j in self.serverLock[ip]:
                        if j in tmp_waitingLock:
                            tmp_waitingLock.remove(j)
                    if tmp_waitingLock:
                        self.waitingLock[ip] = tmp_waitingLock
            
            for ip in self.waitingLock: #self.waitingLock:   {ip1:[A.x-r,B.y-w....]}
                waiting_ip_tmp = set()
                waiting_list = self.waitingLock[ip]          #[A.x-r,B.y-w....]  the source that this ip is waiting for
                for item in waiting_list:                    #item: A.x-r
                    item = item.split("-")[0]                #item:A.x
                    print("item is: ",item)
                    if item in self.source_ip:               #self.source_ip:  {A.x:[ip1,ip2....],B.y:[]...}
                        tmp_ip_list = self.source_ip[item]   #the ips that are acquring the source
                        if ip in tmp_ip_list:
                            tmp_ip_list.remove(ip)
                        waiting_ip_tmp.update(tmp_ip_list)
                self.ip_wait_ip[ip] = waiting_ip_tmp         #ip_wait_ip:  {ip1:set(ip,ip,ip),ip2}
                print("ip_wait_ip is :",self.ip_wait_ip)
            # ip_no_input = []
            # L = len(self.ip_wait_ip)
            tmp_ip_wait_ip = self.ip_wait_ip                #tmp_ip_wait_ip:  {ip1:set(ip,ip,ip),ip2:()}
            while tmp_ip_wait_ip:
                L = []
                # print("---------------------: ",tmp_ip_wait_ip)
                tmp_keys = list(tmp_ip_wait_ip.keys())            #all the ips that are waiting for other ips,it is a list
                tmp_set = set()                             
                for i in tmp_ip_wait_ip:
                    tmp_set = tmp_set | tmp_ip_wait_ip[i]     #all the ips, that someone is waiting for
                for i in tmp_keys:                          
                    if i not in tmp_set:
                        L.append(i)
                if not L:
                    if ip_now in self.serverAddr:
                        self.abortList = (self.serverAddr[ip_now])
                        del self.serverAddr[ip_now]
                    abort_ip = ip_now
                    self.send_msg(abort_ip)
                    print("abort inform list----------------------------------------: ",self.abortList,"\n")
                    print("abort ip is: ",abort_ip)
                    del tmp_ip_wait_ip[abort_ip]
                    del self.serverLock[abort_ip]
                    del self.clinentLock[abort_ip]
                    del self.waitingLock[abort_ip]
                    if abort_ip in self.ip_wait_ip:
                        del self.ip_wait_ip[abort_ip]
                    for key in self.source_ip:   #self.source_ip:  {A.x:[ip1,ip2....],B.y:[]...}
                        if abort_ip == self.source_ip[key]:
                            self.source_ip[key].remove(abort_ip)
                        else:
                            continue
                else:
                    for i in L:
                        del tmp_ip_wait_ip[i]
                    self.abortList.clear()
            print("abort list is: ",self.abortList,"\n" )
            print("severLock list is: ",self.serverLock,"\n" )
            print("ClinentLock list is: ",self.clinentLock,"\n" )
            print("source_ip list is: ",self.source_ip,"\n" )
            print("waitingLock list is: ",self.waitingLock,"\n" )
            print("ip_wait_ip list is: ",self.ip_wait_ip,"\n" )

if __name__ == '__main__':
    host = socket.gethostname()
    # host = "1111.2222.3333.4444"
    port = 9999
    # # print(host)
    # coordinator = Coordinator(host,port)
    coordinator = Coordinator(host,port)
    t1 = threading.Thread(target = coordinator.socket_rcv)
    if not t1.is_alive():
        t1.daemon = True
        t1.start()

    while True:
        pass