import math
import pandas as pd
import random
import pprint
import warnings
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
        self.s = s # predefined scatter width
        self.p = math.ceil(s/(r-1)) # permutation amount 
        self.ideal_s = math.ceil(s/(r-1)) * (r-1) # round s,  p* (r-1),  >= s
        # ideal_s  >= s
        # real s depends on size,  min (n, ideal_s)
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

            left = self.cluster_nodes
            for i in range(self.p):
                new_copyset_id = copyset_seq_number()

                self.current_copysets.add(new_copyset_id)
                if len(left)>= self.r-1:
                    chosen_siblings = set(random.sample(left, k=self.r-1))


                    left = left - chosen_siblings
                # case where ideal s > n
                else:
                    chosen_siblings = left
                    temp_left = self.cluster_nodes - left
                    chosen_siblings += set(random.sample(temp_left, k=(self.r-1 - len(chosen_siblings))))
                new_nodes_df = pd.DataFrame({"copyset_id": [new_copyset_id]*self.r, "node_id": [new_node_id] + list(chosen_siblings)})
                self.copyset_node_relationship = self.copyset_node_relationship.append(new_nodes_df, ignore_index=True)

                # add new node at last
                self.cluster_nodes.add(new_node_id)




        # add new copyset with one node

    def remove_nodes(self, nodes_to_remove):
        # for remove nodes, cyclic assign nodes in one half-full copyset to the next half-full
        for n in nodes_to_remove:
            # should atomic           
            self.cluster_nodes.remove(n) # error if no exist.
        self.copyset_node_relationship.reset_index(drop=True, inplace=True)
        relationships_to_mod = self.copyset_node_relationship[ self.copyset_node_relationship["node_id"].isin(nodes_to_remove)]
        for index, row in relationships_to_mod.iterrows():
            print
            this_copyset_nodes = self.copyset_node_relationship[self.copyset_node_relationship["copyset_id"]==row['copyset_id']]['node_id']
            node_candidates = self.cluster_nodes - set(this_copyset_nodes)
            new_node = random.sample(node_candidates, 1)[0]
            print( "\n\nremoved node_id: %d from copyset %d" % (row['node_id'], row['copyset_id']))
            self.copyset_node_relationship.at[index, "node_id"] = new_node # before ==row['node_id']
            print( "new node id joined %d" % self.copyset_node_relationship.iloc[index].loc["node_id"])

        print("hello")

        # sorted_halffull_copyset = self.relationship_to_mod["copyset_id"].sort_values()
        # s = len(sorted_halffull_copyset)
        # for i, j  in zip(range(s), range(1, 1+s) ):
        #     # put i's left element in copyset j
        #     src_cs_id = sorted_halffull_copyset.iloc[i]
        #     des_cs_id = sorted_halffull_copyset.iloc[j]
        #     self.copyset_node_relationship[self.copyset_node_relationship["node_id"].isin(nodes_to_remove)]



        # add remove from copyset


    def remove_copysets(self, copysets_to_remove):
        for cs in copysets_to_remove:
            # should atomic           
            self.current_copysets.remove(cs) # error if no exist.
        self.copyset_node_relationship = self.copyset_node_relationship[ ~ self.copyset_node_relationship["copyset_id"].isin(copysets_to_remove)]
        # add remove from copyset


    def copyset_count_by_node(self):
        cs_cnt_by_nd = self.copyset_node_relationship.groupby("node_id").count()
        # index is node id
        cs_cnt_by_nd.reset_index(inplace=True)
        cs_cnt_by_nd.rename({"copyset_id":"copyset_cnt"},axis=1,inplace=True)
        return cs_cnt_by_nd

    def node_count_by_copyset(self):
        nd_cnt_by_cs = self.copyset_node_relationship.groupby("copyset_id").count()
        # print(type(nd_cnt_by_cs))
        #
        # print(nd_cnt_by_cs)
        # index is cs id
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
        # index is node_id
        scatter_by_node = scatter_by_node.reset_index()
        scatter_by_node.rename({"node_cnt":"scatter_width"},axis=1,inplace=True)
        return scatter_by_node

    def node_leave_copyset(self):
        copyset_coverage = self.copyset_count_by_node()
        for idx, row in copyset_coverage.iterrows():
            if row["copyset_cnt"] > self.p:
                print("\noverscattered node: %d %d > %d" % (row["node_id"], row["copyset_cnt"], self.p))
                all_copysets = self.copyset_node_relationship.loc[self.copyset_node_relationship["node_id"] == row["node_id"]]
                rm_idx = random.sample(list(all_copysets.index), row["copyset_cnt"] - self.p)
                print("before remove table size %d" % self.copyset_node_relationship.shape[0])
                self.copyset_node_relationship.drop(rm_idx,inplace=True)
                print("after remove table size %d" % self.copyset_node_relationship.shape[0])


    def merge_copyset(self):
        copyset_size = self.node_count_by_copyset()
        halffull_cs = copyset_size[copyset_size["node_cnt"] < self.r]
        halffull_cs.sort_values(by='node_cnt', ascending=False,inplace=True)
        halffull_cs.reset_index(drop=True, inplace=True)

        tail1 = tail0 = halffull_cs.shape[0] - 1

        left_node_cnt = halffull_cs.iloc[tail0]["node_cnt"]
        is_tail_head_met = False
        copyset_migration_mapping = dict() # old_cs_id new_cs_id
        # format
        # {1:1
        #  2:1
        #
        #  33:{1:3, 2:1 } size 3+1
        #
        #

        for head, row in halffull_cs.iterrows():
            print("\n\n")
            print(head)
            # merge biggest with smallest cs
            tail0 = tail1
            if tail1 <= head:
                print("head tail meet here, when head++,\nNo migration, keep last copyset portion nodes")

                break

            merged_size = row["node_cnt"] + left_node_cnt
            if merged_size >= self.r:
                left_node_cnt = merged_size - self.r
                nodes_merged_at_tail0 = self.r - row["node_cnt"]
                nodes_merged_at_tail1 = self.r - row["node_cnt"]
                print(
                    "head copyset: {copyset}\n"
                    "original size: {node_cnt}\n"
                    "tail0 copyset: {tail0}\n"
                    "tail0 nodes mig size: {tail0_n}\n"
                    "tail1 copyset: {tail0}\n"
                    "tail1 nodes mig size: {tail1_n}\n"
                    .format( copyset= row["copyset_id"], \
                     node_cnt = row["node_cnt"], \
                    tail0=halffull_cs.loc[tail0]["copyset_id"],\
                      tail0_n = nodes_merged_at_tail0, \
                    tail1 = halffull_cs.loc[tail1]["copyset_id"],\
                    tail1_n = nodes_merged_at_tail1)
                )
                # print(row["copyset_id"], row["node_cnt"], halffull_cs.loc[tail0]["copyset_id"],
                #       nodes_merged_at_tail0, halffull_cs.loc[tail1]["copyset_id"],nodes_merged_at_tail1)

                self.update_copyset_migration_mapping(copyset_migration_mapping, halffull_cs, row["copyset_id"],
                                                      tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)


                continue

            while merged_size < self.r:

                tail1 -= 1
                merged_size += halffull_cs.loc[tail1]["node_cnt"]
                if tail1 <= head:
                    print("head tail meet here, when tail--")
                    is_tail_head_met = True
                    break

            if is_tail_head_met:
                # print(row["copyset_id"], row["node_cnt"], tail0, nodes_merged_at_tail0, -1, -1)
                print(
                    "head copyset: {copyset}\n"
                    "original size: {node_cnt}\n"
                    "tail0 copyset: {tail0}\n"
                    "tail0 nodes mig size: {tail0_n}\n"
                    "tail1 copyset: {tail0}\n"
                    "tail1 nodes mig size: {tail1_n}\n"
                    .format( copyset= row["copyset_id"], \
                     node_cnt = row["node_cnt"], \
                    tail0=halffull_cs.loc[tail0]["copyset_id"],\
                      tail0_n = nodes_merged_at_tail0, \
                    tail1 = -1,\
                    tail1_n = -1)
                )

                print("last merged copyset size %d" % merged_size)
                self.update_copyset_migration_mapping(copyset_migration_mapping, halffull_cs, row["copyset_id"],
                                                      tail0, nodes_merged_at_tail0, -1,-1)

                break

            nodes_merged_at_tail0 = left_node_cnt
            left_node_cnt = merged_size - self.r
            nodes_merged_at_tail1 = halffull_cs.loc[tail1]["node_cnt"] - left_node_cnt

            # print(row["copyset_id"], row["node_cnt"], tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)
            print(
                "head copyset: {copyset}\n"
                "original size: {node_cnt}\n"
                "tail0 copyset: {tail0}\n"
                "tail0 nodes mig size: {tail0_n}\n"
                "tail1 copyset: {tail0}\n"
                "tail1 nodes mig size: {tail1_n}\n"
                    .format(copyset=row["copyset_id"], \
                            node_cnt=row["node_cnt"], \
                            tail0=halffull_cs.loc[tail0]["copyset_id"], \
                            tail0_n=nodes_merged_at_tail0, \
                            tail1=halffull_cs.loc[tail1]["copyset_id"], \
                            tail1_n=nodes_merged_at_tail1)
            )
            self.update_copyset_migration_mapping(copyset_migration_mapping, halffull_cs, row["copyset_id"],
                                                  tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)

        print("merge plan ")
        pprint.pprint(copyset_migration_mapping)
        print("merge start")
        self.do_migration(copyset_migration_mapping)

    def do_migration(self,migration_mapping):
        for src_cs,dest_cs in migration_mapping.items():
            if isinstance(dest_cs,int) and dest_cs == src_cs:
                continue

            elif isinstance(dest_cs,dict):
                mig_node = self.copyset_node_relationship[self.copyset_node_relationship['copyset_id']==src_cs]
                mig_node_idx = set(mig_node.index)
                for dest,node_size in dest_cs.items():
                    mig_grp = random.sample(mig_node_idx,node_size)
                    self.copyset_node_relationship.loc[mig_grp, "node_id"] = dest
                    mig_node_idx  = mig_node_idx - set(mig_grp)


    def put_node_size_in_migration_mapping(self, mapping, copyset_dest, copyset_src, size):
        if mapping.get(copyset_src) != None:
            if isinstance(mapping.get(copyset_src), dict):
                mapping[copyset_src][copyset_dest] = size
            else:
                warnings.warn("before %d: %d ; after value %s " % (copyset_src, mapping.get(copyset_src), repr({copyset_dest: size})))

        else:
            mapping[copyset_src] = {copyset_dest: size}



    def update_copyset_migration_mapping(self, copyset_migration_mapping, halffull_cs, dest_copyset_id, tail0, nodes_merged_at_tail0,tail1, nodes_merged_at_tail1):
        # print(row["copyset_id"], row["node_cnt"], tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)

        copyset_migration_mapping[dest_copyset_id] = dest_copyset_id


        if tail1 == -1:
            if nodes_merged_at_tail0 != 0:
                self.put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id, halffull_cs.loc[tail0]['copyset_id'], nodes_merged_at_tail0)

            ## todo
            if dest_copyset_id+1 <= tail0-1:
                remain_cs = halffull_cs.loc[dest_copyset_id+1:tail0-1]["copyset_id"]
                for cs in remain_cs:
                    copyset_migration_mapping[cs] = dest_copyset_id
            return

        if tail1 == tail0:
            self.put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id,
                                                    halffull_cs.loc[tail0]['copyset_id'], nodes_merged_at_tail0)
            return

        if tail1< tail0: # todo tail1 size may not needed
            self.put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id,
                                                halffull_cs.loc[tail1]['copyset_id'], nodes_merged_at_tail1)
            if nodes_merged_at_tail0 != 0:
                self.put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id,
                                                        halffull_cs.loc[tail0]['copyset_id'], nodes_merged_at_tail0)

            if tail1 + 1 <= tail0 - 1:
                remain_cs = halffull_cs.loc[tail1 + 1:tail0 - 1]["copyset_id"]
                for cs in remain_cs:
                    copyset_migration_mapping[cs] = dest_copyset_id

            return


# valid r replica copysets
def init_random_copyset(r, n, s):
    node_list = list(range(1,n+1))
    # n * (s/(n-1)) = #cs * r

    double_minimum_copyset =2*math.ceil(n*s/(r*(r -1)))
    repeated_node_list = node_list * math.ceil(double_minimum_copyset/len(node_list))

    copysets = []

    count = 0
    for node in repeated_node_list:
        rand_cs = set(random.sample(set(node_list) - {node}, k=r-1))
        while rand_cs in copysets:
            rand_cs = set(random.sample(set(node_list) - {node}, k=r-1))

        copysets.append(rand_cs | {node})
        count += 1
        if count == double_minimum_copyset:
            break


    return copysets


if __name__ == "__main__":
    CS_3_20_4 = init_random_copyset(5, 40, 7)
    # c = Cluster([[1,2,3],[2,3,4],[3,4,5],
    #             [4,5,6],[5,6,1],[6,1,2],
    #             [1,3,5],[2,4,6]])
    c = Cluster(CS_3_20_4)
    print(c.copyset_node_relationship)

    print("init copyset_node_relationship size %d" % len(c.copyset_node_relationship))
    print(c.cluster_nodes)
    print(c.current_copysets)

    print("\n\n\ncopyset_count_by_node")
    print(c.copyset_count_by_node())

    print("\n\n\nnode_count_by_copyset")
    print(c.node_count_by_copyset())


    print("\n\n\nscatter_width_by_node")
    print(c.scatter_width_by_node())



    d_node = [6, 4]
    c.remove_nodes(d_node)

    print("\n\n\nafter delete node %s, copyset_node_relationship" % repr(d_node))
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)
    print("\n\n\nnode_count_by_copyset")
    print(c.node_count_by_copyset())




    n = 4
    c.add_n_nodes(n)
    print("\n\n\nafter add %d new nodes,  copyset_node_relationship" % n)
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)
    print("\n\n\nnode_count_by_copyset")
    print(c.node_count_by_copyset())


    rm_cs = [7]
    c.remove_copysets(rm_cs)
    print("\n\n\nafter delete copyset %s, copyset_node_relationship" % repr(rm_cs))
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)
    print("\n\n\nnode_count_by_copyset")
    print(c.node_count_by_copyset())

    c.node_leave_copyset()
    c.merge_copyset()
    print("bye~")

