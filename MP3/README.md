# ECE428_MP3

There are 3 files for this MP.

**1. MP3_server.go**

-To run server, type go run MP3_server.go {serverName} {port} {1 or 0}.  
-serverName refers to A or B or C or D or E.  
-Port refers to port. Usually 4444 for A, 5555 for B, 6666 for C, 7777 for D, 8888 for E, it depends on what you set in MP3_client.go from line 13 to 18 (hard code).   
-1 is used to start deadlock detection and 0 indicates no deadlock detection. They are used for convenience to test the function of our code.  
P.S. Start coordinator before start client & server, if you want deadlock detection.  

**2. MP3_client.go**

-To run client, type go run MP3_client.go {1 or 0}.  
-1 is used to start deadlock detection and 0 indicates no deadlock detection. They are used for convenience to test the function of our code.  
-REMEMBER to change the code part from line 13 to 18 to the VM addresses (hard code). What you set for the port needs to be the same when you run server.  
P.S. Start coordinator before start client & server, if you want deadlock detection.  

**3. MP3_coordinator.py**

-To run coordinator, type python MP3_coordinator.py.  
-Hard code on port 9999, if you want to change the coordinator’s addr in MP3_client.go, just change the ip.  
-Our coordinator is written in python3.6. Make sure run it with python3.  

**Notice:**
1. Make sure to run every client on different IP!  
2. Servers can be run on the same IP address with different port.  
3. For your convenience, you may start from testing 0 (no deadlock detection).  
