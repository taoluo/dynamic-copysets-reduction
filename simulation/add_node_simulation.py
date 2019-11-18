from greedy_manager.copysets_manager import CopysetsManager
import matplotlib.pyplot as plt
import numpy as np

experiment = [1,2,3]

x = [[] for _ in range(len(experiment))]
res_sw = [[] for _ in range(len(experiment))]
res = [[] for _ in range(len(experiment))]
start = 1000
gen = CopysetsManager(start, 3, 4)
gen.generate()

base_test = False

x_base = []
res_sw_base = []
res_base = []

for i in experiment:
  step = int(np.ceil(2000/i))
  gen = CopysetsManager(start, 3, 4)
  gen.generate()

  gen_batch = CopysetsManager(start, 3, 4)
  gen_batch.generate()

  for ii in range(step):
    l = gen.mean_sw()
    res_sw[i-1].append(l)
    res[i-1].append(len(gen.copysets))
    gen.add_node(i)


    x[i-1].append(start+ii*i)

    print("{}: {}".format(i,start+ii*i))
    if i == experiment[-1] and base_test:
      gen_base = CopysetsManager(start+ii*i, 3, 4)
      gen_base.generate()
      res_sw_base.append(gen_base.mean_sw())
      res_base.append(len(gen_base.copysets))
      x_base.append(start+ii*i)


plt.style.use("ggplot")
plt.figure()
for i in experiment:
  label = "step"+str(i)
  plt.plot(x[i-1], res_sw[i-1], label=label)
if base_test:
  plt.plot(x_base, res_sw_base, label="base")
plt.xlabel("#nodes")
plt.ylabel("scatter width")
plt.legend(loc="upper left")
plt.savefig('add_node_simulation_sw.png')

plt.style.use("ggplot")
plt.figure()
for i in experiment:
  label = "step"+str(i)
  plt.plot(x[i-1], res[i-1], label=label)
if base_test:
  plt.plot(x_base, res_base, label="base")
plt.xlabel("#nodes")
plt.ylabel("#copysets")
plt.legend(loc="upper left")
plt.savefig('add_node_simulation_copysets.png')
