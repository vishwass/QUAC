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






def trimx(y):  #returns a sorted list of nodes in the given clique 
   
    x = str(y)
    for char in x:
            if char in "(,|)":
                 x = x.replace(char,'')    
    x=''.join(set(x))  
    return ''.join(sorted(x))




def formNodeEdgeTuples(d): # forms tuples with sorted string of nodes as key and set of edges as values
   
    d1 = trimx(str(d))
    return (d1,d)
    
     


def ifmatch(a,b):       # function which checks if 2 cliques have a common vertex or node ,if they have a common vertex then it returns true
   
    a1 = str(trimx(a))
    b1 = str(trimx(b))
    z = False
    for x in a1:
        for y in b1:
            if(x==y):
                z = True
                break 
    return z
     

    
    
def findCombinations(lst):   # function which forms combinations of different cliques having a common vertex or node

    clist = []
        
    for i in range(0,len(lst)):
        for j in range(i+1,len(lst)):
            if(ifmatch(lst[i],lst[j])):       #ifmatch() checks if if both of them have common nodes
                clist.append(lst[i]+"|"+lst[j])
                
    print "combination list  ouput: ",clist
    return clist




def quacConditionChecker(x):   # this function removes quasi cliques which do not satisfy the algorithm condition
    
    global gamma
    global changevar
    global k
    print changevar
    Setx = set()
    keylen = len(x[0])
    y = str(x[1]).split("|")
    
    for i in y:
        Setx.add(i) 

    st = ''
    z=0
    for i in Setx:
        if z>0:
            st = st+"|"+i
        else:
            st = i
            z=1;

    vallength = len(Setx)
    keyval = int(math.factorial(keylen))/(math.factorial(keylen-2)*2)

    if(keylen > k):
        keyval = keyval*gamma 

    print "checking condition\n\n\n:",keyval,vallength,changevar,gamma
    
    if vallength >= keyval:             
        return ("true",st) 
    return (changevar,"r") 




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
        x = '('+fields[0]+','+fields[1]+')'
        C.add(x)

    print C

    while 1:
        changevar = False

        lst = list(C)
        clist  = findCombinations(lst)

        print clist
        mapinput = sc.parallelize(clist)


        secondmr = mapinput.map(formNodeEdgeTuples).reduceByKey(lambda a,b:a+"|"+b).map(quacConditionChecker).collect()


        nextiterationlist = []  #remove cliques from list which doesnt satisfy the condition ,these clique tuple have been identigied been identified with special value 
        for x in secondmr: 
            print x 
            if x[1] != 'r':
                nextiterationlist.append(x)



        print "\n\n\n\n mapreduce output ::::::",nextiterationlist,changevar


        if len(nextiterationlist)==0:  
            break


        temp = set()   
        for x in nextiterationlist:
            cliquelist = x[1].split("|")
            for setA in C:
                set1 = str(setA)
                f = set1.split("|")
                if set(cliquelist)>set(f):
                    temp.add(set1)
            C = C-temp
            C.add(x[1])

        
        if nextiterationlist[0][0]=="true":
            changevar = True
             
        if changevar == False:
            break




    print '\nlist of gamma quasi cliques with gamma=',gamma,'k=',k
    for x in C:
        print x
    print '\n'


  