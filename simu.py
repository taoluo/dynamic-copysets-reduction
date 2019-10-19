import math
import pandas as pd
import random
def node_seq_number():
    node_seq_number.counter += 1
    return node_seq_number.counter
node_seq_number.counter = 0


def copyset_seq_number():
    copyset_seq_number.counter += 1
    return copyset_seq_number.counter
copyset_seq_number.counter = 0


# for i in range(4):
    # print(node_seq_number())

# print("current node max is %d" % node_seq_number.counter)

class Cluster():
    def __init__(self,init_copysets = [{},{}], r=3, s = 4 ):
        # // init_copysets required to be list of set  [{},{}]
        # assume no duplicates of copysets
        init_copysets_dedup = []
        for cs in init_copysets:
            if cs not in init_copysets_dedup:
                init_copysets_dedup.append(cs)

        self.r = r # replica amount
        self.s = s # scatter width
        self.p = math.ceil(s/(r-1)) # permutation amount 
        self.real_s_obj = math.ceil(s/(r-1)) * (r-1) # round s,  p* (r-1),  >= s

        self.cluster_nodes = set()
        for cs in init_copysets_dedup:
            for n in cs:
                self.cluster_nodes.add(n)
        node_seq_number.counter = max(self.cluster_nodes)

        self.current_copysets = set(i+1 for i in range(len(init_copysets_dedup)))
        copyset_seq_number.counter = len(init_copysets_dedup)
        
        self.copyset_node_relationship = pd.DataFrame(columns=["copyset_id", "node_id"])
        next_relation_idx = 0
        for copyset_id_min_1, nodes in enumerate(init_copysets_dedup):
            for n in nodes:
                self.copyset_node_relationship.loc[next_relation_idx] = \
                {"copyset_id": copyset_id_min_1+1, "node_id":n}
                next_relation_idx += 1



    def add_n_nodes(self, number_of_new_nodes):
        for i in range(number_of_new_nodes):
            # shold atomic      
            new_node_id = node_seq_number()
            new_copyset_id = copyset_seq_number()
            self.cluster_nodes.add(new_node_id)
            self.current_copysets.add(new_copyset_id)
            self.copyset_node_relationship = self.copyset_node_relationship.append( {"copyset_id": new_copyset_id, "node_id":new_node_id}, ignore_index=True)


        # add new copyset with one node

    def remove_nodes(self, nodes_to_remove):
        for n in nodes_to_remove:
            # should atomic           
            self.cluster_nodes.remove(n) # error if no exist.
        self.copyset_node_relationship = self.copyset_node_relationship[ ~ self.copyset_node_relationship["node_id"].isin(nodes_to_remove)]
        # add remove from copyset


    def remove_copysets(self, copysets_to_remove):
        for cs in copysets_to_remove:
            # should atomic           
            self.current_copysets.remove(cs) # error if no exist.
        self.copyset_node_relationship = self.copyset_node_relationship[ ~ self.copyset_node_relationship["copyset_id"].isin(copysets_to_remove)]
        # add remove from copyset

    def copyset_count_by_node(self):
        cs_cnt_by_nd = self.copyset_node_relationship.groupby("node_id").count()
        cs_cnt_by_nd.reset_index(inplace=True)
        cs_cnt_by_nd.rename({"copyset_id":"copyset_cnt"},axis=1,inplace=True)
        return cs_cnt_by_nd

    def node_count_by_copyset(self):
        nd_cnt_by_cs = self.copyset_node_relationship.groupby("copyset_id").count()
        # print(type(nd_cnt_by_cs))
        #
        # print(nd_cnt_by_cs)
        
        nd_cnt_by_cs.reset_index(inplace=True)
        nd_cnt_by_cs.rename({"node_id":"node_cnt"},axis=1,inplace=True)
        return nd_cnt_by_cs

    def scatter_width_by_node(self):
        copyset_size = self.node_count_by_copyset()
        # copyset_cnt_by_node = self.copyset_count_by_node()
        
        scatter_df = pd.merge(self.copyset_node_relationship, copyset_size,how="left", on="copyset_id")
        scatter_by_node = scatter_df.groupby("node_id")["node_cnt"].apply(lambda x: x.sum() - x.count())
        # print(type(scatter_by_node))
        #
        # print(scatter_by_node)
        scatter_by_node = scatter_by_node.reset_index()
        scatter_by_node.rename({"node_cnt":"scatter_width"},axis=1,inplace=True)
        return scatter_by_node

def create_random_copyset(r, n, s):
    node_list = list(range(1,n+1))
    double_minimum_copyset =2*math.ceil(n*s/(r*(r -1)))
    copysets = []
    for i in range(double_minimum_copyset):
        rand_cs = set(random.sample(node_list, k=3))
        if rand_cs not in copysets:
            copysets.append(rand_cs)
    return copysets


if __name__ == "__main__":
    CS_3_20_4 = create_random_copyset(3,20,4)
    # c = Cluster([[1,2,3],[2,3,4],[3,4,5],
    #             [4,5,6],[5,6,1],[6,1,2],
    #             [1,3,5],[2,4,6]])
    c = Cluster(CS_3_20_4)

    print(c.cluster_nodes)
    print(c.current_copysets)
    print("init copyset_node_relationship")
    print(len(c.copyset_node_relationship))

    print(c.copyset_node_relationship)

    # c.remove_nodes([6])
    # print("\n\n\nafter delete node 6, copyset_node_relationship")
    # print(c.cluster_nodes)
    # print(c.current_copysets)
    # print(len(c.copyset_node_relationship))
    # print(c.copyset_node_relationship)
    #
    #
    # c.remove_copysets([7])
    # print("\n\n\nafter delete copyset 7, copyset_node_relationship")
    # print(c.cluster_nodes)
    # print(c.current_copysets)
    # print(len(c.copyset_node_relationship))
    # print(c.copyset_node_relationship)
    #
    # c.add_n_nodes(4)
    # print("\n\n\nafter add n new nodes,  copyset_node_relationship")
    # print(c.cluster_nodes)
    # print(c.current_copysets)
    # print(len(c.copyset_node_relationship))
    # print(c.copyset_node_relationship)


    print("\n\n\ncopyset_count_by_node")
    print(c.copyset_count_by_node()) 

    print("\n\n\nnode_count_by_copyset")
    print(c.node_count_by_copyset()) 


    print("\n\n\nscatter_width_by_node")
    print(c.scatter_width_by_node()) 

    print("bye~")

