import os
import matplotlib.pyplot as plt
import numpy as np

whole = []

prop = []

reached = []

with open ('sorted_trans.log', 'r') as f:
	a = f.readlines()
	first = float(a[0].split("-----")[1])
	for i in range(1, len(a)):
		if "service" in a[i]:
			tmp = [i-first for i in prop]
			whole.append(tmp)
			if len(prop) + 1 > 20:
				reached.append(20)
			else:
				reached.append(len(prop) + 1)
			first = float(a[i].split("-----")[1])
			prop = []
		else:
			prop.append(float(a[i].split("-----")[1]))

# avg_prop = [i[1] for i in whole]
median_prop = [np.median(i) for i in whole]
max_prop = [max(i) for i in whole]
min_prop = [min(i) for i in whole]
x = [i for i in range(len(whole))]


plt.figure()
plt.plot(x, max_prop, linewidth=1, label = "max")
plt.plot(x, min_prop, color = 'red', linewidth=1, label = "min")
plt.plot(x, median_prop, color = 'green', linewidth=1, label = "median")
plt.title("Propogation for each transaction")
plt.xlabel("number of transactions")
plt.ylabel("propagation(sec)")
plt.legend(loc='upper right')
plt.show()

plt.figure()
plt.plot(x, reached, linewidth=1, label = "reached node")
plt.xlabel("number of transactions")
plt.ylabel("reached nodes")
plt.title("Number of reached node for each transaction")
plt.show()