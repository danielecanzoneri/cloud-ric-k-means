import random
import sys

from sklearn.datasets import make_blobs
import numpy as np
import matplotlib.pyplot as plt

def generate_dataset(k, d, n):
    X, _ = make_blobs(n_samples=n, n_features=d, centers=k, center_box=(0.0, 30.0))
    np.savetxt('dataset.csv', X, delimiter=',')

    plt.scatter(X[:, 0], X[:, 1])
    plt.show()


if len(sys.argv) != 4:
    print("Usage: python script.py k d n")
    sys.exit(1)

k = int(sys.argv[1])
d = int(sys.argv[2])
n = int(sys.argv[3])

generate_dataset(k, d, n)