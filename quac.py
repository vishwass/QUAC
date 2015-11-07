from pyspark import SparkContext

import time
import itertools
import math
import sys,getopt
import base64
import pickle
from math import *
from itertools import combinations
from collections import namedtuple
from Cluster import Cluster

changevar =True
cliques = []  #the list containing gamma quasi cliques
gamma =0.66 #default value of gamma
k=3   # default value of k-size of the clique after which gamma should be applied


#create each each edge into a clique
def createinitialClusters(input):
    initialcluster= Cluster()
    initialcluster.addedge(input)
    return (hash(initialcluster),initialcluster)


def cliquemap(input):
    lister = []
    for x in input[1].getNodes():
            tup = (str(x),(set([input]),set([]),False))
            lister.append(tup)
    return lister

def prepareNextIterationInput(input):
    print "\n\n\niteration output:\n\n\n"
    print "taskoutput:",input
    lister = []
    newclusters = input[1][1]
    for x in newclusters:
        lister.append(x)
    return lister




def findValidCliques(clist1,clist2):
    global gamma
    global k
    print "find valid cliques"    

    changevar = clist1[2] or clist2[2]

    temp1set = clist1[0]
    temp2set = clist2[0]
    newcandidates =set()
    for x in temp1set:
        for y in temp2set:
            if (x[1].edges.issubset(y[1].edges) or y[1].edges.issubset(x[1].edges)):
                 continue
            clust= x[1].unionCluster(y[1])
            if(clust.isValidClique(gamma,k)):
                 newcandidates.add(clust.formHashClique())
                 if changevar == False:
                     changevar =True

    set1 = temp1set.union(temp2set)
    set2 = newcandidates.union(clist1[1])
    set2 = set2.union(clist2[1])
    set2 = removesubsets(set2)
    result = (set1,set2,changevar)
    return result


# Given a set of clusters ,removes all the clusters which are subsets of other clusters in the input set
def removesubsets(inputset):
    subsets =set()
    for x in inputset:
        for y in inputset:
            if x[1] != y[1]:
                if x[1].isSubsetOf(y[1]):
                    subsets.add(x)
    
    return inputset-subsets



def combineTheReducers(input1,input2):

    rlist1 =set()
    rlist2 =set()
    #tree reduce remove redundant clusters
    for cluster1 in input1[1][0]:
        for cluster2 in input2[1][0]:
            if(cluster1[1].isSubsetOf(cluster2[1])):
                rlist1.add(cluster1)
            elif (cluster2[1].isSubsetOf(cluster1[1])):
                rlist2.add(cluster2)

    for x in rlist1:
        input1[1][0].remove(x)
    for y in rlist2:
        input2[1][0].remove(y)
    set1 = input1[1][0].union(input2[1][0])
    set2 = input1[1][0].union(input2[1][0])

    change = input1[1][2] or input2[1][2]

    return (str(0),(set1,set2,change))



def returnChangevar(input1,input2):

    change = input1[1][2] or input2[1][2]
    return (str(0),(set(),set(),change))



 #main function    
if __name__ == "__main__":

    #global variables
    global changevar
    global C
    global gamma
    global k

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

    beg = time.time()

    sc = SparkContext(appName="quasicliqueEnumeration", pyFiles=['Cluster.py'])


    #creating initial set of clusters
    clusters= sc.textFile("edges.g3").map(createinitialClusters)

    iteration = 0
    while(changevar):
        changevar = False
        clusters = clusters.flatMap(cliquemap).reduceByKey(findValidCliques)
        changevar = clusters.reduce(returnChangevar)[1][2]

        iteration = iteration+1
        print "/////////iteration number:",iteration,"/////////"
        print "changevar:",changevar
        if(changevar):
            clusters = clusters.flatMap(prepareNextIterationInput)
            

       
    clusters =clusters.treeReduce(combineTheReducers)

    print '\nlist of gamma quasi cliques with gamma=',gamma,'k=',k,"iterations=",iteration
    print clusters
    for x in clusters[1][1]:
            x[1].clusterPrint()

    sc.stop()
  
    end = time.time();
    print "time =", (end - beg)
