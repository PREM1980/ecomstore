'''
Created on Apr 26, 2013

@author: temp_plakshmanan
'''
S=range(10000)
T=range(300)



import time
t1 = time.time()

testlist =[x for x in S if x in T]

t2 = time.time()

print t2-t1 

t1 = time.time()

testlista =filter(lambda x:x in S,T)

t2 = time.time()

print t2-t1 