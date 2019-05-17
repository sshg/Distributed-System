import os
import matplotlib.pyplot as plt
import numpy as np

whole = []
with open ('bandwidth_node4.log', 'r') as f:
	a = f.readlines()
	for i in a:
		whole.append(round(float(i.split("-----")[0])))

x = [i for i in range(len(whole))]
plt.figure()
plt.plot(x, whole, linewidth=1)
plt.title("bandwidth of one node")
plt.xlabel("number of transactions")
plt.ylabel("bandwidth (trans per sec)")
plt.show()