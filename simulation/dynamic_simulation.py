from greedy_manager.copysets_manager import CopysetsManager
import matplotlib.pyplot as plt
import random
import numpy as np

N = 1000
R = 3
S = 4

x = []
copysets_numbers = []

steps = 500

gen = CopysetsManager(N, R, S)
gen.generate()

for i in range(steps):
    print(i)
    x.append(i)
    copysets_numbers.append(len(gen.copysets))

    diff = gen.N // 20
    gen.add_node(diff)

    nodes = gen.get_node_indices()
    indices = random.sample(nodes, k=diff)
    for index in indices:
        gen.remove_node(index)


# plt.style.use("ggplot")
plt.figure()
plt.plot(x, copysets_numbers, 'r.')
plt.xlabel("#nodes")
plt.ylabel("#copysets")
plt.savefig('dynamic_sim.png')
