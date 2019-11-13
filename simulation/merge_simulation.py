from scipy.special import comb
from cluster_simu.cluster import Cluster
import matplotlib.pyplot as plt
import random
import numpy as np


N = 1000
R = 3
S = 4

steps = 500
c = Cluster(N, R, S, init = 'greedy')

for i in range(steps):
    c.add_n_nodes(1)

print("#copysets before merge: {}".format(len(c.greedy_copysets)))

c.merge()

print("#copysets after first merge: {}".format())

