'''

Authors: Vishwas Shanbhog ,Rustom Shroff

University At Buffalo
CSE 603 Masters Project - Enumeration of Gamma Quasi Clique

Instructor: Professor Jaroslaw Zola

#Main reference to our implementation
#Reference :http://aluru-sun.ece.iastate.edu/lib/exe/fetch.php?media=papers:closet-wp.pdf

'''
from pyspark import SparkContext
from pyspark.accumulators import AccumulatorParam
from pyspark import StorageLevel
from pyspark.serializers import MarshalSerializer

from bisect import *
import time
import sys,getopt
from math import *

#custom accumulator for boolean variable changevar
class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return False
    def addInPlace(self, val1, val2):
        return val1 or val2


sc = SparkContext(appName="quasicliqueEnumeration",serializer=MarshalSerializer())


#changevar check whether new clusters are formed and decides whether to go to the next iteration
changevar= sc.accumulator(bool, VectorAccumulatorParam())

gamma =0.9#default value of gamma

k=3   # default value of k-size of the clique after which gamma should be applied


#create each each edge into a clique
def createinitialClusters(input):
    nodes = input.split()
    edge = ()
    if len(nodes) > 1:
        if(nodes[1] > nodes[0]):
            edge = (int(nodes[0]),int(nodes[1]))
        else:
            edge = (int(nodes[1]),int(nodes[0]))
        cluster = [edge]
        a = (hash(tuple(getNodes(cluster))),[cluster,False])
        return a

#map clusters to its nodes
def cliqueMap(input):
    lister = []
    if input == None:
        return lister
    for x in getNodes(input[1][0]):
        tup = (x,input)
        lister.append(tup)
    return lister

#get unique sorted nodes for an edgelist
def getNodes(edgelist):
    nodes = []
    #print "edgelist inside getnodes",edgelist
    for x in edgelist:
        i = bisect_left(nodes,x[0])
        if i == len(nodes) or nodes[i]!=x[0]:
            nodes.insert(i,x[0])
        j = bisect_left(nodes,x[1])
        if j == len(nodes) or nodes[j]!=x[1]:
            nodes.insert(j,x[1])
    return nodes

#cluster edgelist union
def listUnion(list1,list2):
    l = []
    for x in list2:
        i= bisect_left(l,x)
        if i ==len(l) or l[i] != x:
            l.insert(i,x)
    for x in list1:
        i= bisect_left(l,x)
        if i ==len(l) or l[i] != x:
            l.insert(i,x)
    return l



# find newcluster by joining clusters
def findCliques(clist):
    global gamma
    global k
    newclustedgelist = []
    flag = []
    iteration =0
    falselist = []

    clusterlist = list(clist[1])

    if len(clusterlist)== 1:
        return [clusterlist[0]]

    for i in range(0,len(clusterlist)):
        flag.append(False)

    while (1):

        iteration +=1

        for i in range(0,len(clusterlist)):
            for j in range(i+1,len(clusterlist)):

                newclustedges= listUnion(clusterlist[i][1][0],clusterlist[j][1][0])
                newclustnodes= getNodes(newclustedges)

                numnodes = len(newclustnodes)
                numcombinations = numnodes*(numnodes-1)/2
                d = 2*len(newclustedges)/numcombinations
                merge = True
                if  d >= gamma:
                    if numnodes > k:
                        #optimization Reference :http://aluru-sun.ece.iastate.edu/lib/exe/fetch.php?media=papers:closet-wp.pdf
                        count = []
                        for i in range(0,len(newclustnodes)):
                            count.append(0)
                        for j in range(0,len(newclustedgelist)):
                            for i in range(0,len(newclustnodes)):
                                if newclustedgelist[j][0] ==newclustnodes[i] or newclustedgelist[j][1] == newclustnodes[i]:
                                    count[i] +=1
                            for x in count:
                                if x <2:
                                    merge = False
                        else:
                            merge = False
                else:
                    merge = False


                if merge ==True:
                    if clusterlist[i] in clist[1]:
                        clusterlist[i][1][1] = True
                        falselist.append(clusterlist[i])
                    if clusterlist[j] in clist[1]:
                        clusterlist[j][1][1] = True
                        falselist.append(clusterlist[j])
                    newclustedgelist.append((hash(tuple(newclustnodes)),[newclustedges,False]))
                    flag[i] = True
                    flag[j] = True



        emit =True
        for i in range(0,len(flag)):
            if flag[i] ==False:
                newclustedgelist.append(clusterlist[i])
            else:
                emit = False
        
        #no new cluster formed
        if emit ==True:
            print "numclusters:",len(newclustedgelist)
            return newclustedgelist+falselist
        else:
            clustnodes =[]
            for i in range(0,len(newclustedgelist)):
                clustnodes.append((i,getNodes(newclustedgelist[i][1][0])))
            clustnodes.sort(key = lambda x:x[1])
            #print "new clusteredgelist",newclustedgelist

            pos =0
            clusterlist = []
            for i in range(1,len(clustnodes)):
                if clustnodes[pos][1]!=clustnodes[i][1]:
                    edges = []
                    for j in range(pos,i):
                        edges = listUnion(edges,newclustedgelist[clustnodes[j][0]][1][0])
                    clusterlist.append((newclustedgelist[clustnodes[pos][0]][0],[edges,False]))
                    pos = i
            #last batch merge
            edges = []
            for j in range(pos,len(clustnodes)):
                edges = listUnion(edges,newclustedgelist[clustnodes[j][0]][1][0])
            clusterlist.append((newclustedgelist[clustnodes[pos][0]][0],[edges,False]))
            clustnodes = []

        if len(clusterlist)>2000:
            print "numclusters:",len(clusterlist)
            return clusterlist+falselist

        newclustedgelist = []
        flag = []
        for x in range(0,len(clusterlist)):
            flag.append(False)



#simple merge efficient but doesnt take care of hash key collision
#note: to be used with reduceByKey eg: rdd.reduceByKey(mergeClusters)
def mergeClusters(a,b):
    if a[1] ==True:
        return b
    elif b[1]== True:
        return b
    else:
        return [listUnion(a[0],b[0]),False]


#mergeclusters
#note:to be used with combineByKey usage: rdd.combineByKey(createCombiner,mergeValue,mergeCombiners)
def createCombiner(a):
    return [a]


def mergeValue(comb,val):
    flag =0
    if val[1] ==True:
        for x in comb:
            if getNodes(x[0]) ==getNodes(val[0]):
                x[0] = listUnion(x[0],val[0])
                x[1] = True
                flag =1
                break
        if flag ==0:
            comb.append(val)
        return comb

    for x in comb:
        if getNodes(x[0]) == getNodes(val[0]):
            flag =1
    if flag ==0:
        comb.append(val)
    return comb


def mergeCombiners(comb1,comb2):
    comb1.extend(comb2)
    if len(comb1)==0:
        return comb1

    nodes = []
    for i in range(0,len(comb1)):
        nodes.append((i,getNodes(comb1[i][0])))

    nodes.sort(key = lambda a:a[1])
    clusters =[]
    pos=0
    for i in range(1,len(nodes)):
        if nodes[pos][1] != nodes[i][1]:
            clust =[]
            flag = False
            for j in range(pos,i):
                clust = listUnion(clust,comb1[nodes[j][0]][0])
                flag = flag or comb1[nodes[j][0]][1]
            clusters.append([clust,flag])
            pos =i
    clust =[]
    flag = False
    for j in range(pos,len(nodes)):
        clust = listUnion(clust,comb1[nodes[j][0]][0])
        flag = flag or comb1[nodes[j][0]][1]
    clusters.append([clust,flag])

    return clusters

#refactor clusters for next iteration
def prepareNextIteration(a):
    return [ (a[0],x) for x in a[1] ]


#update accumulator
def updateChangeVar(a):
    global changevar
    changevar += a[1][1]


 #main function    
if __name__ == "__main__":

    #global variables
    global gamma
    global k
    global changevar

    # command line argument processing
    try:                                                      
        opts, args = getopt.getopt(sys.argv[1:],"g:k:")
    except getopt.GetoptError:
        print './pyspark quac.py -g <gammavalue> -k <k value>'
        sys.exit(2)
    for opt, arg in opts:  
        if opt == "-g":
            gamma = float(arg)
        elif opt =="-k":
            k = int (arg)
        elif opt =="-y":
            print '\n\nusage:'
            print './pyspark quac.py -g <gammavalue> -k <k value>\n\n'
            sys.exit(1)

    print 'gamma value =',gamma
    print 'k value =', k

    if gamma > 0.95 or gamma < 0.3 :
        print "Invalid gamma value.exiting....."
        sys.exit()

    beg = time.time()


    #creating initial set of clusters
    clusters= sc.textFile("edges.g3").map(createinitialClusters)
    iter = 0
    changevar.value = True
    while(changevar.value):
        changevar.value  =False
        iter+=1

        #find new clusters
        clusters = clusters.flatMap(cliqueMap).groupByKey(numPartitions=2).flatMap(findCliques).persist(StorageLevel.MEMORY_AND_DISK_SER)

        #update accumulator variable changevar
        clusters.foreach(updateChangeVar)
        print "\n@@@@@@@@@@@changevar: iteration",changevar.value,iter
        #clusters.unpersist()

        #merge same node clusters and remove merged clusters
        clusters = clusters.combineByKey(createCombiner,mergeValue,mergeCombiners).flatMap(prepareNextIteration).filter(lambda a:a[1][1] != True)

        #print iteration time
        end = time.time()
        print "iteration:time =",iter,":",(end - beg)


    clusters = clusters.saveAsTextFile("500_g0.9clusters4n48ex1excors")
    sc.stop()
    end = time.time()
    print "total time =", (end - beg)
