from functools import cmp_to_key


"""
Cuurent implementation has R as 3 internally
"""
class CopysetsManager:
  def __init__(self, N=9, R=3, S=4):
    if R != 3:
      raise NotImplementedError
    self.N = N
    self.R = R
    self.S = S
    self.copysets = set()
    self.scatter_dict = {x:0 for x in range(N)}
    self.node_counter = self.N

  def reset(self):
    self.copysets = set()
    self.scatter_dict = {x:0 for x in range(N)}
    self.node_counter = self.N

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
    for sets in self.copysets:
      if node in sets:
        for n in sets:
          members.add(n)
    if node in members:
      members.remove(node)
    return members

  """
  Generate copysets based on current parameters(N, R, S, Scatter Width List)
  """
  def generate(self):
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
    while scatter_list[0][1] < self.S:
      nodes = []

      # Select first node
      for i in range(self.N):
        node_i = scatter_list[i][0]
        nodes.append(node_i)
        members_i = self.get_copysets_member(node_i)
        # Add penalty to existing copysets members
        add_penalty(members_i)

        # Sort again
        scatter_list_i = sorted_dict()
        for j in range(self.N):
          # Node index could change
          if scatter_list_i[j][0] == node_i:
            continue

          node_j = scatter_list_i[j][0]
          nodes.append(node_j)
          members_j = self.get_copysets_member(node_j)
          add_penalty(members_j)

          scatter_list_j = sorted_dict()
          for k in range(self.N):
            if scatter_list_j[k][0]in (node_i, node_j):
              continue

            node_k = scatter_list_j[k][0]
            nodes.append(node_k)
            # Found new copysets
            if tuple(sorted(nodes)) in self.copysets:
              nodes.remove(nodes[-1])
              continue
            else:
              break

          # Remove panalty for current node
          remove_penalty(members_j)
          if len(nodes) == self.R:
            break
          else:
            nodes.remove(nodes[-1])

        remove_penalty(members_i)
        if len(nodes) == self.R:
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

      self.copysets.add(tuple(sorted(nodes)))
      scatter_list = sorted_dict()

  """
  Add n nodes at same time and regenerate
  """
  def add_node(self, n=1):
    self.N += n
    for i in range(self.node_counter, self.node_counter + n):
      self.scatter_dict[i] = 0
    self.node_counter += n
    self.generate()

  """
  Remove node with specific index
  gen.remove_node(1)   Node 1 is not avaliable after
  """
  def remove_node(self, node):
    if node not in self.scatter_dict:
      print("Node {} does not exist".format(node))
      return
    members = self.get_copysets_member(node)
    self.copysets = set(copyset for copyset in self.copysets if node not in copyset)

    for m in members:
      new_members = self.get_copysets_member(m)
      self.scatter_dict[m] = len(new_members)
    self.scatter_dict.pop(node)
    self.N -= 1
    self.generate()

  def show(self):
    tmp = sorted([list(c) for c in self.copysets])
    print("copysets: ", tmp)
    print("len: ", len(self.copysets))

  def show_sw(self):
    l = [(k, v) for k,v in self.scatter_dict.items()]
    print(self.sort_scatter_list(l))

  def mean_sw(self):
    l = [v for k,v in self.scatter_dict.items()]
    return sum(l)/len(l)

  def median_sw(self):
    l = [v for k,v in self.scatter_dict.items()]
    return l[int(len(l)/2)]

  def get_node_indices(self):
    l = [k for k,v in self.scatter_dict.items()]
    return l


# parameters: N, R, S
# R is not used (set as 3)
if __name__ == "__main__":
    gen = CopysetsManager(9, 3, 4)
    gen.generate()
    gen.show()
    gen.show_sw()
