import math
import pandas as pd

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
    def __init__(self,init_copysets = [[],[]], r=3, s = 4 ):
        # // init_copysets required to be list of list  [[][]]
        # assume no duplicates of copysets
        self.r = r # replica amount
        self.s = s # scatter width
        self.p = math.ceil(s/(r-1)) # permutation amount 
        self.real_s_obj = math.ceil(s/(r-1)) * (r-1) # round s,  p* (r-1),  >= s

        self.cluster_nodes = set()
        for cs in init_copysets:
            for n in cs:
                self.cluster_nodes.add(n)
        node_seq_number.counter = max(self.cluster_nodes)


        self.current_copysets = set(i+1 for i in range(len(init_copysets))) 
        copyset_seq_number.counter = len(init_copysets)
        
        self.copyset_node_relationship = pd.DataFrame(columns=["copyset_id", "node_id"])
        next_relation_idx = 0
        for copyset_id_min_1, nodes in enumerate(init_copysets):
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
        print(type(nd_cnt_by_cs))

        print(nd_cnt_by_cs)
        
        nd_cnt_by_cs.reset_index(inplace=True)
        nd_cnt_by_cs.rename({"node_id":"node_cnt"},axis=1,inplace=True)
        return nd_cnt_by_cs

    def scatter_width_by_node(self):
        copyset_size = self.node_count_by_copyset()
        # copyset_cnt_by_node = self.copyset_count_by_node()
        
        scatter_df = self.copyset_node_relationship.join(copyset_size,on="copyset_id",how="left",lsuffix="",rsuffix="_sz")
        scatter_by_node = scatter_df.groupby("node_id")["node_cnt"].sum() - scatter_df.groupby("node_id")["node_cnt"].count()
        print(type(scatter_by_node))

        print(scatter_by_node)
        scatter_by_node.reset_index(inplace=True)
        scatter_by_node.rename({"node_cnt":"scatter_width"},axis=1,inplace=True)
        return scatter_by_node

if __name__ == "__main__":
    c = Cluster([[1,2,3],[2,3,4],[3,4,5],
                [4,5,6],[5,6,1],[6,1,2],
                [1,3,5],[2,4,6]])

    print(c.cluster_nodes)
    print(c.current_copysets)
    print("init copyset_node_relationship")
    print(len(c.copyset_node_relationship))

    print(c.copyset_node_relationship)

    c.remove_nodes([6])
    print("\n\n\nafter delete node 6, copyset_node_relationship")
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)


    c.remove_copysets([7])
    print("\n\n\nafter delete copyset 7, copyset_node_relationship")
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)

    c.add_n_nodes(4)
    print("\n\n\nafter add n new nodes,  copyset_node_relationship")
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)


    print("\n\n\ncopyset_count_by_node")
    print(c.copyset_count_by_node()) 

    print("\n\n\nnode_count_by_copyset")
    print(c.node_count_by_copyset()) 


    print("\n\n\nscatter_width_by_node")
    print(c.scatter_width_by_node()) 



