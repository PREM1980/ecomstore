'''
Created on Apr 26, 2013

@author: temp_plakshmanan
'''


testdict={'name':'Tom','age':20}

testdict['salary']=300

print 'adding salary=',testdict

del testdict['salary']

print 'deleting salary=',testdict

print 'Updating dict'
testdict['age']=30


for x, y in testdict.items():
    print x,y

for x in testdict.keys():
    print x
    
for x in testdict.values():
    print y    
    

testdict2=dict( (a,b) for a,b in zip(range(2),['a','b','c']) )
print testdict2          


    if a=2:
