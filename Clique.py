

class Clique:

    def __init__(self):
        self.edges = set()
        self.nodes = set()


    def update(self,edge,node1,node2):
        self.edges.add(edge)
        self.nodes.add(node1)
        self.nodes.add(node2)


    def unionClique(self,clique2):
        self.edges = self.edges.union(clique2.edges)
        self.nodes = self.nodes.union(clique2.nodes)
        return self


