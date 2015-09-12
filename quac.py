from pyspark import SparkContext
sc = SparkContext("local[4]", "quasicliqueEnumeration", pyFiles=['quac.py'])


import itertools
import math
import sys,getopt
import base64
from math import *
from itertools import combinations
from collections import namedtuple


changevar =True
initialCvar = True
C = set()  #the set containing gamma quasi cliques
gamma =0.66 #default value of gamma
k=3   # default value of k-size of the clique after which gamma should be applied


# forms tuples with hash of edge as key and edges themselves as tuples
def formHashEdgeTuples(d):
    return (hash(d),d)


 # forms tuples with source as key and edge as value
def formSourceEdgeTuples(d):
    print "inside formsourceedgetuples",d
    nodes = d[1].split(",",1)
    return (nodes[0],[d[1]])


# forms tuples with destination node as the key and edge as the value
def formDestinationEdgeTuples(d):
    print "inside formdestinationedgetuples",d
    nodes = d[1].split(",",1)
    return (nodes[1],[d[0]])


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


    #lines as a list
    textFile = sc.textFile("edges.g3").collect()

     #intializing the first set of cliques
    first=''
    fields =[]
    edge = ''
    for edge in textFile:
        fields = edge.split(" ")
        x = str(fields[0]+','+fields[1])
        C.add(x)

    print C


    lst = list(C)
    firstmapinput = sc.parallelize(lst)
    firstmroutput = firstmapinput.map(formHashEdgeTuples).collect()


    secondmapinput =  sc.parallelize(firstmroutput)
    secondmr1 =  secondmapinput.map(formSourceEdgeTuples).collect()
    secondmr2 = secondmapinput.map(formDestinationEdgeTuples).collect()


    creatingthirdmapinput = secondmr1+secondmr2
    thirdmrinput = sc.parallelize(creatingthirdmapinput)
    thirdmroutput = thirdmrinput.reduceByKey(lambda a,b:a+b).collect()


    print '\nlist of gamma quasi cliques with gamma=',gamma,'k=',k
    for x in thirdmroutput:
        print x
        print '\n'


  