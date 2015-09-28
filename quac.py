from pyspark import SparkContext
sc = SparkContext("local[4]", "quasicliqueEnumeration", pyFiles=['quac.py'])


import itertools
import math
import sys,getopt
import base64
import pickle
from math import *
from itertools import combinations
from collections import namedtuple
from Clique import Clique

changevar =True
cliques = []  #the list containing gamma quasi cliques
gamma =0.66 #default value of gamma
k=3   # default value of k-size of the clique after which gamma should be applied




# forms tuples with destination node as the key and edge as the value
def formSourceDestinationEdgeTuples(d):
    print "inside formdestinationedgetuples\n",d
    nodes = d.split(" ")
    edge = str(nodes[0])+","+str(nodes[1])
    initialclique1= Clique()
    initialclique1.update(edge,nodes[0],nodes[1])
    lst =[]
    print "nodes[0]",nodes[0]
    a=(nodes[0],initialclique1)
    b=(nodes[1],initialclique1)
    lst.append(a)
    lst.append(b)
    return lst


def maptest(a):
    print a
    return a


def cliqueUnion(a,b):
    c = Clique()
    c.edges =  a.edges|b.edges
    c.nodes =  a.nodes|b.nodes
    return c

def cliquemap(input):
    lister = []
    for x in input[1].nodes:
        tup = (x,input[1])
        lister.append(tup)
        print "tup",tup[1].nodes
    return lister

def mergeCliques(input1,input2):
    global changevar
    global gamma
    global cliques

    newcliq = cliqueUnion(input1,input2)

    nnodes = len(newcliq.nodes)

    if len(newcliq.edges) >= gamma * (nnodes*(nnodes-1)/2):
        changevar =True
        return newcliq

    # if the criterion is not satisified send an empty clique as it does not form a gamma quasi clique
    #y = Clique()
    return input2









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


    #creating adjacency list
    cliques= sc.textFile("edges").flatMap(formSourceDestinationEdgeTuples).reduceByKey(lambda a,b:cliqueUnion(a,b)).collect()

    print "initial cliques"
    for x in cliques :
        print x[1].edges

    cliques = sc.parallelize(cliques)

    while(changevar):
        changevar = False
        cliques = cliques.flatMap(cliquemap).reduceByKey(mergeCliques)


    cliques = cliques.collect()




    print '\nlist of gamma quasi cliques with gamma=',gamma,'k=',k
    for x in cliques :
        print "clique",x
        print x[1].edges
        print '\n'


  
