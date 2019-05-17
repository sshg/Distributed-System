# ECE428_MP2 checkpoint2

MP2.go is the main program. Others are script and logfile

**1. MP2.go**

Main program  
  
-Download the file from URL link  
-Update it to the VMs  
-Type go MP2.go {node number} {ip} {port} to start
-Stop service node and see "finished" means the program is over. 
-Never mind if you see some "Server Error to read message because of XXX" when stopping the service node. It's used for debugging.  
-Type "cat *_Trans.csv > total.csv" to generate total log file for analysis 
  
See report for more details.  

**2. analyzer.ipynb**

Script used to analyze "total.csv".

**P.S. "total.csv" is the log I used to draw pics in report**