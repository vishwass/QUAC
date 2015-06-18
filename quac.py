from pyspark import SparkContext
sc = SparkContext("local", "gamaquasi1")    
from sets import Set
import itertools
import math
from math import *
from collections import namedtuple


changevar =True
C =  set()

initialCvar = True
textFile = sc.textFile("edges")
linesWithSpark = textFile.filter(lambda line: "(" in line)
firstmr = textFile.map(lambda line: (1,line)).reduceByKey(lambda a, b: a+"/"+b).collect()
print firstmr
a = firstmr[0][1]
b = a.split("/")
for s in b:
    C.add(s)
print C


def trimx(y):
    print "inside trim", y
    x = str(y)
    print "type is",type(x)
    for char in x:
            if char in "(,|)":
                 x = x.replace(char,'')    
    x=''.join(set(x))  
    print "output trimx function:",x       
    return ''.join(sorted(x))

def m2func(d):
    print "inside m2func :",d,type(d)
    d1 = trimx(str(d))
    print d1,d
    a = (d1,d)
    print "output m2func:",a,type(a) 
    return a
    

def ifmatch(a,b):
    print "inside ifmatch:"
    a1 = str(trimx(a))
    b1 = str(trimx(b))
    print a1,b1,type(a1),type(b1)
    z = False
    for x in a1:
        for y in b1:
            if(x==y):
                print x
                print y
                z = True
                break 
    print "output ifmatch: ",a,b,z            
    return z
                
    
    
def mr2func(line):
   
    print "inside mr2func:",line
    sets= line[1].split("/")
    global initialCvar
    if initialCvar == True:
        initialCvar= False
          for h in sets:
            C.add(h)

    lst = []  
    finallst  = []
    
    for s in sets:
        print s
        lst.append(s)
        
        
    for i in range(0,len(lst)):
        for j in range(i+1,len(lst)):
            if(ifmatch(lst[i],lst[j])):
                finallst.append(lst[i]+"|"+lst[j])
                
    print "flatmap list ouput: ",finallst            
    return finallst    


def mapvalue(x):
    print "inside mapvaluefunc:",x
    Setx = set()
    global changevar
    print changevar
    keylen = len(x[0])
    y = str(x[1]).split("|")
    
    for i in y:
        Setx.add(i)
    st = str() 
    z=0   
    for i in Setx:
        print i
        if z>0:
            st = st+"|"+i
        else:
            st = i
            z=1;
    vallength = len(Setx)
    keyval = math.factorial(keylen)/(math.factorial(keylen-2)*2)
    if vallength >= keyval:             
        print "checking condition\n\n:",st,changevar  
        return ("true",st) 
    print changevar    
    return (changevar,"r") 

f1n = sc.parallelize(firstmr)
while 1:
    changevar = False
    secondmr = f1n.flatMap(mr2func).map(m2func).reduceByKey(lambda a,b:a+"|"+b).map(mapvalue).collect()
    nextiter = []
    for x in secondmr:
        print x
        
        if x[1] != 'r':
            print "x[1]:",x[1]
            nextiter.append(x)
    print "\n\n mapreduce output ::::::",nextiter,changevar,"\n\n"
    if len(nextiter)==0:
        break
    temp = set()
    for x in nextiter:
        cliquelist = x[1].split("|")
        for setA in C:
            set1 = str(setA)
            f = set1.split("|")
            if set(cliquelist)>set(f):
                temp.add(set1)
        C = C-temp
        C.add(x[1])
    
    if nextiter[0][0]=="true":
        print "inside s \n\n\n\n\n"
        changevar = True
        
        
    
        
    if changevar == False:
        break
   
    lst =[]
    sty = str()
    j=0
    for i in range(0,len(nextiter)):
        if j==0:
            sty=sty+nextiter[i][1]
            j=j+1
        else:   
            sty=sty+"/"+nextiter[i][1] 
        
    print sty         

    lst.append((1,sty)) 
    
    f1n = sc.parallelize(lst)

print C