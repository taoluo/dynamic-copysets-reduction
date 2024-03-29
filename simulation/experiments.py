from scipy.special import comb
from cluster_simu.cluster import Cluster
import matplotlib.pyplot as plt
import random
import logging
import numpy as np

logging.basicConfig(level=logging.DEBUG)

def get_copysets_member(node, copysets):
    members = set()
    for k,sets in copysets.items():
        if node in sets:
            for n in sets:
                members.add(n)
    if node in members:
        members.remove(node)
    return members

def check_internal_dup(copysets_dict):
    copyset = set()
    nodes = set()
    for k,v in copysets_dict.items():
        node_set = set(v)
        if len(v) != 3:
            logging.debug("{} has illegal size {} with {}".format(k, len(v), v))
        if len(node_set) < 3:
            logging.debug("{} might have duplicated node {}".format(k, v))
        if tuple(sorted(v)) in copyset:
            logging.debug("duplicated copyset {}".format(v))
        else:
            copyset.add(tuple(sorted(v)))
        for n in v:
            nodes.add(n)

    scatter_dict = dict()
    for n in nodes:
        members = get_copysets_member(n, copysets_dict)
        scatter_dict[n] = len(members)
    for k,v in scatter_dict.items():
        if v < 4:
            logging.debug("node {} has scatter width {}".format(k, v))


N = 9
R = 4
S = 4

steps = 20
c = Cluster(N, R, S, init = 'greedy')
print(c.greedy_copysets)

count = 0
x = []
copysets_number = []

#c = Cluster(N, R, S, init = 'greedy')
#
#for i in range(steps):
#    print(count)
#    x.append(count)
#    count += 1
#    diff = c.n // 100
#    if diff < 1:
#        diff = 1
#    c.add_n_nodes(diff)
#    nodes = list(c.get_node_indices())
#    indices = random.sample(nodes, k=diff)
#    c.remove_nodes(indices)
#    copysets_number.append(len(c.greedy_copysets))
#
#logging.debug("#copysets before merge: {}".format(len(c.greedy_copysets)))
#
#c.node_leave_copyset()
#c.merge_copyset()
#
#logging.debug("estimated cost: {}".format(c.merge_cost))
#logging.debug("#copysets after first merge: {}".format(c.get_copysets_number()))
##print("#copysets generated by greedy: {}".format(c_base.get_copysets_number()))
#
#copysets_dict = c.get_and_set_greedy_copysets()
#
#check_internal_dup(copysets_dict)

