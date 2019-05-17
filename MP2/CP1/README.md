# ECE428_MP2

MP2.py is the main program. Others are script used to analyze log file and draw pics.

**1. MP2.py**

Main program  
  
-Download the file from URL link  
-Update it to the VMs  
-Type python MP2.py CONNECT {node number} {ip} {port} to start  
  
See report for more details.  

**2. file.sh**

-Put all the "transaction_nodeX.log" files together  
-run file.sh to get "sorted_trans.log" for further analysis  

**3. file_analyzer.py**

Used to draw propagation and reached nodes pic 
  
-put "sorted_trans.log" to current directory  
-run "python file_analyzer.py" to generate figures  

**3. bandwidth_analyze.py**

Used to draw bandwidth pic 

-choose one "bandwidth_nodeX.log"  
-run "python bandwidth_analyze.py" to generate figures  

**P.S. "sorted_trans.log" and "bandwidth_node4.log" are logs I used to draw pics in report**