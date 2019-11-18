from greedy_manager.copysets_manager import CopysetsManager
import matplotlib.pyplot as plt
import random
import numpy as np

N = 1000
R = 3
S = 4

x = []
copysets_numbers = []

nodes_numbers = []

steps = 1000

gen = CopysetsManager(N, R, S)
gen.generate()

for i in range(steps):
    print(i)
    x.append(i)
    nodes_numbers.append(gen.N)
    copysets_numbers.append(len(gen.copysets))

    diff = gen.N // 100

    diff_div = random.randint(1,5)
    gen.add_node(diff)

    nodes = gen.get_node_indices()
    diff_div = random.randint(1,5)
    remove_diff = diff
    if remove_diff >= len(nodes):
        remove_diff = 0
    indices = random.sample(nodes, k=remove_diff)
    for index in indices:
        gen.remove_node(index)

print("nodes in cluster: {}".format(gen.N))

# plt.style.use("ggplot")
plt.figure()
plt.plot(x, copysets_numbers, 'r.')
plt.xlabel("time")
plt.ylabel("#copysets")
plt.savefig('dynamic_sim.png')
