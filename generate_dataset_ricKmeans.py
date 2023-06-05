import random
import sys

from sklearn.datasets import make_blobs
import numpy as np
import matplotlib.pyplot as plt

fig = plt.figure()
ax = fig.add_subplot()

def generate_dataset(k, d, n, cluster_std=1.0):
    X, _ = make_blobs(n_samples=n, n_features=d, centers=np.array([(-76.128,4.862),(15.764,83.112),(-21.652,-26.235),(37.907,-59.19),(5.776,25.39)]),
    	center_box=(-100.0, 100.0),
    	cluster_std=cluster_std)
    
    np.savetxt('dataset.csv', X, delimiter=',')

    ax.scatter(X[:100000, 0], X[:100000, 1])
    plt.show()


if len(sys.argv) < 4:
    print("Usage: python script.py k d n (cluster_std)")
    sys.exit(1)

k = int(sys.argv[1])
d = int(sys.argv[2])
n = int(sys.argv[3])
if len(sys.argv) == 5:
	cluster_std = float(sys.argv[4])
else:
	cluster_std = 1.0

generate_dataset(k, d, n, cluster_std)
