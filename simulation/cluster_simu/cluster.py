import math
import pandas as pd
import random
import pprint
import warnings
import pickle
import logging
from functools import cmp_to_key


logging.basicConfig(level=logging.DEBUG)

DBG = 0

class Cluster():
    def __init__(self, n = 9, r = 3, s = 4, init = 'greedy'):
        """

        :param init_copysets:
        :param r:  replica amount
        :param s: predefined scatter width
        """
        self.n = n
        self.r = r
        self.s = s
        self.method = init
        self.p = math.ceil(s/(r-1)) # permutation amount 
        self.ideal_s = math.ceil(s/(r-1)) * (r-1) # round s,  p* (r-1),  >= s
        # ideal_s  >= s
        # real s depends on size,  min (n, ideal_s)
        self.cluster_nodes = set(range(n))
        self.node_seq_counter = max(self.cluster_nodes)
        self.copyset_node_relationship = pd.DataFrame(columns=["copyset_id", "node_id"])

        self.scatter_dict = {x:0 for x in range(n)}
        self.node_counter = self.n
        self.greedy_copysets = set()

        init_copysets = []

        if self.method == 'random':
            init_copysets = self.random_generate_copysets(n, r, s)
        elif self.method == 'greedy':
            if self.r != 3:
                raise NotImplementedError
            init_copysets = self.greedy_generate_copysets(n, r, s)
        else:
            logging.error('init {} not supported'.format(init))
            return
        # // init_copysets required to be list of set  [{},{}]
        # assume no duplicates of copysets

        self.current_copysets = set(i+1 for i in range(len(init_copysets)))

        self.copyset_seq_counter = len(init_copysets)
        # maintains copyset- node relationship, main data structure of class
        next_relation_idx = 0
        for copyset_id_min_1, nodes in enumerate(init_copysets):
            for n in nodes:
                self.copyset_node_relationship.loc[next_relation_idx] = \
                {"copyset_id": copyset_id_min_1+1, "node_id":n}
                next_relation_idx += 1

    def copyset_seq_number(self):
        """
        generate copyset ID sequentially. starts from 0
        :return:
        """
        self.copyset_seq_counter += 1
        return self.copyset_seq_counter

    def node_seq_number(self):
        """
        generate node ID sequentially. starts from 0
        :return:
        """
        self.node_seq_counter += 1
        return self.node_seq_counter

    def dedup_copyset(self):
        """
        if dup copyset exist, return 1, dedup copyset
        if dup copyset not exist, return 0
        :return:
        """

        copyset_table = self.get_copyset_table()
        copyset_cnt = copyset_table.groupby("copyset_tuple").count() # dataframe index is tuple
        collision_copysets = copyset_cnt[copyset_cnt["copyset_id"]!=1]
        if len(collision_copysets) != 0:
            for copyset_tuple, _ in copyset_cnt.iterrows():
                print("dupcate copyset %s" % repr(copyset_tuple))
                dup_copysets = copyset_table[copyset_table.loc[:, "copyset_tuple"] == copyset_tuple]
                # remove dup copysets with greater ID
                copyset_to_remove = dup_copysets["copyset_id"].sort_values()[1:]
                self.remove_copysets(copyset_to_remove)
            return 1
        else:
            return 0



    def get_copyset_table(self):
        """

        :return: dataframe copyset_id copyset_tuple
        """
        copyset_table = self.copyset_node_relationship.groupby("copyset_id").apply(
            lambda x:tuple(x["node_id"].sort_values()))
        copyset_table.name = "copyset_tuple"
        copyset_table = copyset_table.reset_index(drop=False)
        return copyset_table

    def random_generate_copysets(self, n, r, s):
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
        # Deduplication
        init_copysets_dedup = []
        for cs in copysets:
            if cs not in init_copysets_dedup:
                init_copysets_dedup.append(cs)

        return init_copysets_dedup

    """
    Sort node list by individual scatter width and return list
    """
    def sort_scatter_list(self, l):
        l.sort(key=cmp_to_key(lambda a, b: a[1] - b[1]))
        return l

    """
    Get members in same copysets with node i (excluding node i)
    (1,2,3) (1,5,6) -> get members of 1 -> [2,3,5,6]
    """
    def get_copysets_member(self, node):
        members = set()
        for sets in self.greedy_copysets:
            if node in sets:
                for n in sets:
                    members.add(n)
        if node in members:
            members.remove(node)
        return members

    def mean_sw(self):
        l = [v for k,v in self.scatter_dict.items()]
        return sum(l)/len(l)

    def get_node_indices(self):
        l = [k for k,v in self.scatter_dict.items()]
        return l

    def greedy_generate_step(self):
        def sorted_dict():
          l = [(k, v) for k,v in self.scatter_dict.items()]
          return self.sort_scatter_list(l)

        def add_penalty(members):
          for m in members:
            self.scatter_dict[m] += 1

        def remove_penalty(members):
          for m in members:
            self.scatter_dict[m] -= 1

        scatter_list = sorted_dict()

        # Generate till minimal SW satisfies requirement
        while scatter_list[0][1] < self.s:
            nodes = []

            # Select first node
            for i in range(self.n):
                node_i = scatter_list[i][0]
                nodes.append(node_i)
                members_i = self.get_copysets_member(node_i)
                # Add penalty to existing copysets members
                add_penalty(members_i)

                # Sort again
                scatter_list_i = sorted_dict()
                for j in range(self.n):
                    # Node index could change
                    if scatter_list_i[j][0] == node_i:
                        continue

                    node_j = scatter_list_i[j][0]
                    nodes.append(node_j)
                    members_j = self.get_copysets_member(node_j)
                    add_penalty(members_j)

                    scatter_list_j = sorted_dict()
                    for k in range(self.n):
                        if scatter_list_j[k][0]in (node_i, node_j):
                            continue

                        node_k = scatter_list_j[k][0]
                        nodes.append(node_k)
                        # Found new copysets
                        if tuple(sorted(nodes)) in self.greedy_copysets:
                            nodes.remove(nodes[-1])
                            continue
                        else:
                            break

                    # Remove panalty for current node
                    remove_penalty(members_j)
                    if len(nodes) == self.r:
                        break
                    else:
                        nodes.remove(nodes[-1])

                remove_penalty(members_i)
                if len(nodes) == self.r:
                    break
                else:
                    nodes.remove(nodes[-1])

            for node in nodes:
                members = self.get_copysets_member(node)
                sw = len(nodes)-1
                for n in nodes:
                    if n in members:
                        sw -= 1
                self.scatter_dict[node] += sw

            self.greedy_copysets.add(tuple(sorted(nodes)))
            scatter_list = sorted_dict()

    def greedy_generate_copysets(self, n, r, s):
        self.greedy_generate_step()
        return [set(x) for x in self.greedy_copysets]


    def add_n_nodes(self, number_of_new_nodes):
        """
        add n nodes to cluster, node ID is assigned continousely
        :param number_of_new_nodes:
        :return:
        """
        self.n += number_of_new_nodes
        if self.method == 'random':
            for j in range(number_of_new_nodes):
                new_node_id = self.node_seq_number()

                left = self.cluster_nodes
                runout_sample = False
                for j in range(self.p):
                    new_copyset_id = self.copyset_seq_number()

                    self.current_copysets.add(new_copyset_id)
                    if not runout_sample:
                        if len(left)>= self.r-1:
                            chosen_siblings = set(random.sample(left, k=self.r-1))


                            left = left - chosen_siblings
                        # case where ideal s > n
                        else:
                            runout_sample = True
                            chosen_siblings = left
                            temp_left = self.cluster_nodes - left
                            chosen_siblings += set(random.sample(temp_left, k=(self.r-1 - len(chosen_siblings))))
                    else:

                        chosen_siblings = set(random.sample(left, k=self.r-1))

                    new_nodes_df = pd.DataFrame({"copyset_id": [new_copyset_id]*self.r, "node_id": [new_node_id] + list(chosen_siblings)})
                    self.copyset_node_relationship = self.copyset_node_relationship.append(new_nodes_df, ignore_index=True)

                    # add new node at last
                    self.cluster_nodes.add(new_node_id)
        elif self.method == 'greedy':
            for _ in range(number_of_new_nodes):
                new_node_id = self.node_seq_number()
                self.cluster_nodes.add(new_node_id)
                self.scatter_dict[new_node_id] = 0
            old_copysets = self.greedy_copysets.copy()
            self.greedy_generate_step()
            diff_copysets = self.greedy_copysets - old_copysets
            copyset_ids = []
            node_ids = []
            for copyset in diff_copysets:
                new_copyset_id = self.copyset_seq_number()
                copyset_ids.extend([new_copyset_id] * self.r)
                for node in copyset:
                    node_ids.append(node)
            new_nodes_df = pd.DataFrame({"copyset_id": copyset_ids, "node_id": node_ids})
            self.copyset_node_relationship = self.copyset_node_relationship.append(new_nodes_df, ignore_index=True)




        # add new copyset with one node

    def remove_nodes(self, nodes_to_remove):
        """

        :param nodes_to_remove: list of node ID to be removed
        :return:
        """
        # for remove nodes, cyclic assign nodes in one half-full copyset to the next half-full
        if self.method == 'random':
            for n in nodes_to_remove:
                self.n -= 1
                # should atomic           
                self.cluster_nodes.remove(n) # error if no exist.
            self.copyset_node_relationship.reset_index(drop=True, inplace=True)
            relationships_to_mod = self.copyset_node_relationship[ self.copyset_node_relationship["node_id"].isin(nodes_to_remove)]
            for index, row in relationships_to_mod.iterrows():
                this_copyset_nodes = self.copyset_node_relationship[self.copyset_node_relationship["copyset_id"]==row['copyset_id']]['node_id']
                node_candidates = self.cluster_nodes - set(this_copyset_nodes)
                new_node = random.sample(node_candidates, 1)[0]
                print( "\n\nremoved node_id: %d from copyset %d" % (row['node_id'], row['copyset_id']))
                self.copyset_node_relationship.at[index, "node_id"] = new_node # before ==row['node_id']
                print( "new node id joined %d" % self.copyset_node_relationship.iloc[index].loc["node_id"])
        elif self.method == 'greedy':
            for node in nodes_to_remove:
                if node not in self.scatter_dict:
                    logging.error("Node {} does not exist".format(node))
                    continue
                members = self.get_copysets_member(node)
                old_copysets = self.greedy_copysets.copy()
                self.greedy_copysets = set(copyset for copyset in self.greedy_copysets if node not in copyset)
                diff_copysets = old_copysets - self.greedy_copysets
                for copyset in diff_copysets:
                    relationships_tmp = self.copyset_node_relationship[self.copyset_node_relationship["node_id"].isin(list(copyset))]
                    relationships_tmp = relationships_tmp.groupby("copyset_id").count()
                    relationships_tmp.rename({"node_id":"copyset_cnt"}, axis=1, inplace=True)
                    deleted_copyset_id = relationships_tmp[relationships_tmp["copyset_cnt"] == 3].index[0]
                    self.copyset_node_relationship.drop(self.copyset_node_relationship[self.copyset_node_relationship["copyset_id"] == deleted_copyset_id].index, inplace = True)
                for m in members:
                    new_members = self.get_copysets_member(m)
                    self.scatter_dict[m] = len(new_members)
                self.scatter_dict.pop(node)
                self.n -= 1

                old_copysets = self.greedy_copysets.copy()
                self.greedy_generate_step()
                diff_copysets = self.greedy_copysets - old_copysets
                copyset_ids = []
                node_ids = []
                for copyset in diff_copysets:
                    new_copyset_id = self.copyset_seq_number()
                    copyset_ids.extend([new_copyset_id] * self.r)
                    for node in copyset:
                        node_ids.append(node)
                new_nodes_df = pd.DataFrame({"copyset_id": copyset_ids, "node_id": node_ids})
                self.copyset_node_relationship = self.copyset_node_relationship.append(new_nodes_df, ignore_index=True)


        # sorted_halffull_copyset = self.relationship_to_mod["copyset_id"].sort_values()
        # s = len(sorted_halffull_copyset)
        # for i, j  in zip(range(s), range(1, 1+s) ):
        #     # put i's left element in copyset j
        #     src_cs_id = sorted_halffull_copyset.iloc[i]
        #     des_cs_id = sorted_halffull_copyset.iloc[j]
        #     self.copyset_node_relationship[self.copyset_node_relationship["node_id"].isin(nodes_to_remove)]



        # add remove from copyset


    def remove_copysets(self, copysets_to_remove):
        """

        :param copysets_to_remove: list of copyset ID to be removed
        :return:
        """
        for cs in copysets_to_remove:
            # should atomic           
            self.current_copysets.remove(cs) # error if no exist.
        self.copyset_node_relationship = self.copyset_node_relationship[ ~ self.copyset_node_relationship["copyset_id"].isin(copysets_to_remove)]
        # add remove from copyset


    def get_copyset_count_by_node(self):
        """
        count the amount of copysets each nodes belongs to
        :return: a dataframe, columns are ["copyset_cnt" , "node_id"]
        """
        cs_cnt_by_nd = self.copyset_node_relationship.groupby("node_id").count()
        # index is node id
        cs_cnt_by_nd.reset_index(inplace=True)
        cs_cnt_by_nd.rename({"copyset_id":"copyset_cnt"},axis=1,inplace=True)
        return cs_cnt_by_nd

    def get_node_count_by_copyset(self):
        """
        count nodes each copyset contains
        :return: a dataframe, columns are ["node_cnt" , "copyset_id"]
        """
        nd_cnt_by_cs = self.copyset_node_relationship.groupby("copyset_id").count()
        # print(type(nd_cnt_by_cs))
        #
        # print(nd_cnt_by_cs)
        # index is cs id
        nd_cnt_by_cs.reset_index(inplace=True)
        nd_cnt_by_cs.rename({"node_id":"node_cnt"},axis=1,inplace=True)
        return nd_cnt_by_cs

    def get_scatter_width_by_node(self):
        """
        calcualte the scatter width of each node
        :return:  a dataframe, columns are ["scatter_width" , "node_id"]
        """
        copyset_size = self.get_node_count_by_copyset()
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
        """
        API merge copyset algorithm step 1
        for each node that has scatter width greater than ideal scatter width (belongs to more copysets than scatter width needs to)
        .make it randomly leave some of the copysets to makes scatter width just enough.
        :return:
        """
        copyset_coverage = self.get_copyset_count_by_node()
        for idx, row in copyset_coverage.iterrows():
            if row["copyset_cnt"] > self.p:
                if DBG:
                    print("\noverscattered node: %d %d > %d" % (row["node_id"], row["copyset_cnt"], self.p))
                all_copysets = self.copyset_node_relationship.loc[self.copyset_node_relationship["node_id"] == row["node_id"]]
                rm_idx = random.sample(list(all_copysets.index), row["copyset_cnt"] - self.p)
                if DBG:
                    print("before remove table size %d" % self.copyset_node_relationship.shape[0])
                self.copyset_node_relationship.drop(rm_idx,inplace=True)
                if DBG:
                    print("after remove table size %d" % self.copyset_node_relationship.shape[0])


    def merge_copyset(self):
        """
        API merge copyset algorithm step 2
        for each copyset that has less nodes than required replication level. try to merge with others into a
         full copyset (size=self.r)
         until all of these copysets are merged into full copysets.
         well may not all, but leave last copyset not full.

        :return:
        """
        copyset_size = self.get_node_count_by_copyset()
        halffull_cs = copyset_size[copyset_size["node_cnt"] < self.r]
        halffull_cs.sort_values(by='node_cnt', ascending=False,inplace=True)
        halffull_cs.reset_index(drop=True, inplace=True)

        tail1 = tail0 = halffull_cs.shape[0] - 1

        left_node_cnt = halffull_cs.iloc[tail0]["node_cnt"]
        is_tail_head_met = False
        copyset_migration_mapping = dict() # old_cs_id new_cs_id
        # mapping to guide migration of nodes between copyset
        # format
        # {1:1  # no migration
        #  2:1 # migrate all nodes in copyset 2 to copyset 1
        #
        #  33:{1:3, 2:1 } node 33 has 3+1=4 nodes, migrate 3 nodes to copyset 1, and 1 nodes to copyset 2.
        #
        #

        for head, row in halffull_cs.iterrows():
            print("\n")
            print("moving headtail (%d, %d)" % (head, tail1))
            # merge biggest with smallest cs
            print("left_node_cnt %d" % left_node_cnt)
            if tail1 <= head + 1:# or (left_node_cnt==0 and tail1 -head ==2):
                print("head tail meet here, when head++,\nNo migration, keep last copyset portion nodes")
                # TODO merge  last copyset
                break

            tail0 = tail1


            merged_size = row["node_cnt"] + left_node_cnt
            if merged_size >= self.r:
                left_node_cnt = merged_size - self.r
                nodes_merged_at_tail0 = self.r - row["node_cnt"]
                nodes_merged_at_tail1 = self.r - row["node_cnt"]
                if DBG:
                    print(
                        "head copyset: {copyset}\n"
                        "original size: {node_cnt}\n"
                        "tail0 copyset: {tail0}\n"
                        "tail0 nodes mig size: {tail0_n}\n"
                        "tail1 copyset: {tail1}\n"
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

                self.__update_copyset_migration_mapping(copyset_migration_mapping, halffull_cs, row["copyset_id"],
                                                        tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)


                continue

            while merged_size < self.r:
                if tail1-1 <= head:
                    print("head tail meet here, when tail--")
                    is_tail_head_met = True
                    break
                tail1 -= 1
                print("moving headtail (%d, %d)" % (head, tail1))

                merged_size += halffull_cs.loc[tail1]["node_cnt"]


            if is_tail_head_met:
                # print(row["copyset_id"], row["node_cnt"], tail0, nodes_merged_at_tail0, -1, -1)
                if DBG:
                    print(
                        "head copyset: {copyset}\n"
                        "original size: {node_cnt}\n"
                        "tail0 copyset: {tail0}\n"
                        "tail0 nodes mig size: {tail0_n}\n"
                        "tail1 copyset: {tail1}\n"
                        "tail1 nodes mig size: {tail1_n}\n"
                        .format( copyset= row["copyset_id"], \
                         node_cnt = row["node_cnt"], \
                        tail0=halffull_cs.loc[tail0]["copyset_id"],\
                          tail0_n = nodes_merged_at_tail0, \
                        tail1 = -1,\
                        tail1_n = -1)
                    )

                print("last merged copyset size %d" % merged_size)
                self.__update_copyset_migration_mapping(copyset_migration_mapping, halffull_cs, row["copyset_id"],
                                                        tail0, nodes_merged_at_tail0, -1, -1)

                break

            nodes_merged_at_tail0 = left_node_cnt
            left_node_cnt = merged_size - self.r
            nodes_merged_at_tail1 = halffull_cs.loc[tail1]["node_cnt"] - left_node_cnt
            if DBG:
            # print(row["copyset_id"], row["node_cnt"], tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)
                print(
                    "head copyset: {copyset}\n"
                    "original size: {node_cnt}\n"
                    "tail0 copyset: {tail0}\n"
                    "tail0 nodes mig size: {tail0_n}\n"
                    "tail1 copyset: {tail1}\n"
                    "tail1 nodes mig size: {tail1_n}\n"
                        .format(copyset=row["copyset_id"], \
                                node_cnt=row["node_cnt"], \
                                tail0=halffull_cs.loc[tail0]["copyset_id"], \
                                tail0_n=nodes_merged_at_tail0, \
                                tail1=halffull_cs.loc[tail1]["copyset_id"], \
                                tail1_n=nodes_merged_at_tail1)
                )
            self.__update_copyset_migration_mapping(copyset_migration_mapping, halffull_cs, row["copyset_id"],
                                                    tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)



        print("merge plan ")
        pprint.pprint(copyset_migration_mapping)
        print("merge start")
        self.__do_migration(copyset_migration_mapping)
        self.dedup_copyset()

    def __do_migration(self, migration_mapping):
        """
        do migration, change the copyset ID of some nodes based on migration_mapping
        :param migration_mapping:
        # mapping to guide migration of nodes between copyset
        # format
        # {1:1  # no migration
        #  2:1 # migrate all nodes in copyset 2 to copyset 1
        #
        #  33:{1:3, 2:1 } node 33 has 3+1=4 nodes, migrate 3 nodes to copyset 1, and 1 nodes to copyset 2.
        :return:
        """
        for src_cs,dest_cs in migration_mapping.items():
            if isinstance(dest_cs,int):
                if dest_cs == src_cs:
                # No migration for this case
                    continue
                else: # dest_cs != src_cs
                # No migration for this case
                    mig_nodes = self.copyset_node_relationship[self.copyset_node_relationship['copyset_id']==src_cs]
                    current_dest_cs = set(self.copyset_node_relationship[self.copyset_node_relationship['copyset_id'] \
                                                                         == dest_cs]["node_id"])
                    for idx, row in mig_nodes:
                        if row["node_id"] not in current_dest_cs:
                            self.copyset_node_relationship.loc[idx, "copyset_id"] = dest
                            current_dest_cs.add(row["node_id"])
                        else:
                            # merge dup node id by remove relationship
                            self.copyset_node_relationship.drop(idx, inplace=True)

            elif isinstance(dest_cs,dict):
                mig_node = self.copyset_node_relationship[self.copyset_node_relationship['copyset_id']==src_cs]
                mig_node_idx = set(mig_node.index) # all node to be migrated
                for dest,node_size in dest_cs.items():
                    # BUG sample larger than nodesize
                    mig_nodes_idx_sample = random.sample(mig_node_idx,node_size) # list

                    current_dest_cs = set(self.copyset_node_relationship[self.copyset_node_relationship['copyset_id'] \
                                                                         == dest]["node_id"])
                    for idx,row in self.copyset_node_relationship.loc[mig_nodes_idx_sample].iterrows():
                        if row["node_id"] not in current_dest_cs:
                            self.copyset_node_relationship.loc[idx, "copyset_id"] = dest
                            current_dest_cs.add(row["node_id"])
                        else:
                            # merge dup node id by remove relationship
                            self.copyset_node_relationship.drop(idx, inplace=True)
                    # nodes left to be migrated.
                    mig_node_idx  = mig_node_idx - set(mig_nodes_idx_sample)


    def __put_node_size_in_migration_mapping(self, mapping, copyset_dest, copyset_src, size):
        """
        update mapping to put portion (size) of nodes in copyset_src to copyset_dest
        :param mapping: migration mappingf
        :param copyset_dest:
        :param copyset_src:
        :param size:
        :return:
        """
        if mapping.get(copyset_src) != None:
            if isinstance(mapping.get(copyset_src), dict):
                mapping[copyset_src][copyset_dest] = size
            else:
                warnings.warn("before %d: %d ; after value %s " % (copyset_src, mapping.get(copyset_src), repr({copyset_dest: size})))

        else:
            mapping[copyset_src] = {copyset_dest: size}



    def __update_copyset_migration_mapping(self, copyset_migration_mapping, halffull_cs, dest_copyset_id, tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1):
        """
        migrate all nodes in dest_copyset_id, nodes in copysets between tail0 and tail1, may include portion of nodes at tail0 and tail1
        to dest_copyset_id.

        :param copyset_migration_mapping: the mapping to be updated
        :param halffull_cs: dataframe of copyset less than replication level r, columns ["node_cnt" , "copyset_id"], sorted by "node_cnt" descending,
        :param dest_copyset_id: the copyset all nodes are going to be placed into
        :param tail0: rear idx to halffull copyset
        :param nodes_merged_at_tail0: the amount of nodes to be merged in the copyset of tail0
        :param tail1: front idx to halffull copyset
        :param nodes_merged_at_tail1: the amount of nodes to be merged in the copyset of tail1
        :return:
        """

        # print(row["copyset_id"], row["node_cnt"], tail0, nodes_merged_at_tail0, tail1, nodes_merged_at_tail1)

        copyset_migration_mapping[dest_copyset_id] = dest_copyset_id


        if tail1 == -1:
            if nodes_merged_at_tail0 != 0:
                self.__put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id, halffull_cs.loc[tail0]['copyset_id'], nodes_merged_at_tail0)

            ## todo
            if dest_copyset_id+1 <= tail0-1:
                remain_cs = halffull_cs.loc[dest_copyset_id+1:tail0-1]["copyset_id"]
                for cs in remain_cs:
                    copyset_migration_mapping[cs] = dest_copyset_id
            return

        if tail1 == tail0:
            self.__put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id,
                                                      halffull_cs.loc[tail0]['copyset_id'], nodes_merged_at_tail0)
            return

        if tail1< tail0: # todo tail1 size may not needed
            self.__put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id,
                                                      halffull_cs.loc[tail1]['copyset_id'], nodes_merged_at_tail1)
            if nodes_merged_at_tail0 != 0:
                self.__put_node_size_in_migration_mapping(copyset_migration_mapping, dest_copyset_id,
                                                          halffull_cs.loc[tail0]['copyset_id'], nodes_merged_at_tail0)

            if tail1 + 1 <= tail0 - 1:
                remain_cs = halffull_cs.loc[tail1 + 1:tail0 - 1]["copyset_id"]
                for cs in remain_cs:
                    copyset_migration_mapping[cs] = dest_copyset_id

            return


if __name__ == "__main__":
    n = 9
    r = 3
    s = 4
    c = Cluster(n, r, s, init = 'greedy')
    print(c.greedy_copysets)

    # for method in ("greedy", "random"):
    method = "random"
    print(method)
    n = 40
    r = 5
    s = 8
    c = Cluster(n, r, s, init = method)
    # c.dedup_copyset()
    # c.get_node_count_by_copyset()


   # r = 5
   # s = 8
   # n =40
   # CS_3_20_4 = init_random_copyset(r, n, s)
   # # c = Cluster([[1,2,3],[2,3,4],[3,4,5],
   # #             [4,5,6],[5,6,1],[6,1,2],
   # #             [1,3,5],[2,4,6]])
   # # ff = open("CS_3_20_4.pkl", 'rb')
   # # CS_3_20_4 = pickle.load(ff)
   # c = Cluster(CS_3_20_4,r=r,s=s)

    print(c.copyset_node_relationship)

    print("init copyset_node_relationship size %d" % len(c.copyset_node_relationship))
    print(c.cluster_nodes)
    print(c.current_copysets)

    print("\n\n\ncopyset_count_by_node")
    print(c.get_copyset_count_by_node())

    print("\n\n\nnode_count_by_copyset")
    print(c.get_node_count_by_copyset())


    print("\n\n\nscatter_width_by_node")
    print(c.get_scatter_width_by_node())



    d_node = [6, 4]
    c.remove_nodes(d_node)

    print("\n\n\nafter delete node %s, copyset_node_relationship" % repr(d_node))
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)
    print("\n\n\nnode_count_by_copyset")
    print(c.get_node_count_by_copyset())




    n = 4
    c.add_n_nodes(n)
    print("\n\n\nafter add %d new nodes,  copyset_node_relationship" % n)
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)
    print("\n\n\nnode_count_by_copyset")
    print(c.get_node_count_by_copyset())


    rm_cs = [7]
    c.remove_copysets(rm_cs)
    print("\n\n\nafter delete copyset %s, copyset_node_relationship" % repr(rm_cs))
    print(c.cluster_nodes)
    print(c.current_copysets)
    print(len(c.copyset_node_relationship))
    print(c.copyset_node_relationship)
    print("\n\n\nnode_count_by_copyset")
    print(c.get_node_count_by_copyset())



    c.node_leave_copyset()

    print("\nbefore merge: node_count_by_copyset")
    print(c.get_node_count_by_copyset())
    # print(c.current_copysets)

    c.merge_copyset()

    print("\nafter merge: node_count_by_copyset")
    print(c.get_node_count_by_copyset())

    print(c.current_copysets)
    print("\n\n\nafter merge: copyset_count_by_node")
    print(c.get_copyset_count_by_node())


    print("\n\n\nafter merge: scatter_width_by_node")
    print(c.get_scatter_width_by_node())

    c.merge_copyset()

    print("\nafter 2nd merge: node_count_by_copyset")
    print(c.get_node_count_by_copyset())


    print("\nafter 2nd merge: node_count_by_copyset")
    print(c.get_node_count_by_copyset())

    print(c.current_copysets)
    print("\n\n\nafter 2nd merge: copyset_count_by_node")
    print(c.get_copyset_count_by_node())


    print("\n\n\nafter 2nd merge: scatter_width_by_node")
    print(c.get_scatter_width_by_node())


    print("bye~")

