import random
import sys

from sklearn.datasets import make_blobs
import numpy as np
import matplotlib.pyplot as plt

fig = plt.figure()
ax = fig.add_subplot(projection='3d')

def generate_dataset(k, d, n, cluster_std=1.0):
    X, _ = make_blobs(n_samples=n, n_features=d, centers=k,
    	center_box=(-100.0, 100.0),
    	cluster_std=cluster_std)
    
    np.savetxt('dataset.csv', X, delimiter=',')

    ax.scatter(X[:, 0], X[:, 1], X[:, 2])
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
