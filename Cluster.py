

class Cluster:


    def __init__(self):
        self.edges = set()
        self.nodes = set()
        self.key = None

    def __hash__(self):
        return hash(frozenset(self.edges))

    def __eq__(self, other):
        return self.edges == other.edges

    def __repr__(self):
        return "<Cluster \n nodes:%s \n edges:%s>" % (self.nodes, self.edges)

    def __str__(self):
        return "Cluster: nodes = %s  edges = %s" % (self.nodes, self.edges)

    def clusterPrint(self):
        print "Cluster details"
        print "Cluster nodes:",self.nodes
        print "Cluster edges:",self.edges


    def addedge(self,input):
        nodes = input.split(" ")
        if len(nodes) >1:
            edge = str(nodes[0])+","+str(nodes[1])
            self.nodes.add(nodes[0])
            self.nodes.add(nodes[1])
            self.edges.add(edge)


    
    def equals(self,cluster):
        if cluster.nodes== self.edges:
            return True

        return False


    def formHashClique(self):
        return (hash(self),self)
    
    
    def getNodes(self):
        return self.nodes

    
    
    def getMaxNode(self):
        max = 0
        for x in self.nodes:
            if(x> max):
                max =x;
        return max


    def isSubsetOf(self,cluster):
        if(self.edges.issubset(cluster.edges)):
            return True
        return False


    def isValidClique(self,gamma,k):
        #print "checking condition numedges=",self.numEdges(),"numnodes=",self.numNodes()

        if(self.numNodes() > k):
            if self.numEdges() >= gamma * (self.numNodes()*(self.numNodes()-1)/2):
                return True
            else:
                return False

        return True



    def numNodes(self):
        return len(self.nodes)


    def numEdges(self):
        return len(self.edges)


    def unionCluster(self,cluster):
        newclust =Cluster()
        newclust.edges = self.edges|cluster.edges
        newclust.nodes = self.nodes|cluster.nodes
        return newclust


