from copysets_gen import CopysetsManager
from scipy.special import comb
import matplotlib.pyplot as plt
import random

print("Add and Remove steps vs Mean Scatter Width")
steps = 500
gen = CopysetsManager(100, 3, 4)
gen.generate()
res = []

for i in range(steps):
  res.append(gen.mean_sw())
  diff = random.randint(1, 10)
  gen.add_node(diff)
  nodes = gen.get_node_indices()
  indices = random.sample(nodes, k=diff)
  for index in indices:
    gen.remove_node(index)


plt.plot(range(steps), res, 'r.')
plt.ylabel('Mean Scatter Width')
plt.xlabel('time')
plt.show()



print("Data Loss Prob. vs Number of Nodes")
x = []
res = []

for i in range(500, 3000, 50):
  gen = CopysetsManager(i, 3, 4)
  gen.generate()

  l = gen.mean_sw()
  ll = len(gen.copysets) / comb(i, 3)

  x.append(i)
  res.append(ll)


plt.plot(x, res, 'b.')
plt.ylabel('Data Loss Prob.')
plt.xlabel('#Node')
plt.show()
