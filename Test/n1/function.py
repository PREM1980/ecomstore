'''
Created on Apr 26, 2013

@author: temp_plakshmanan

'''
#This is for positional parameter
def func(a,b,c):
    print 'a=',a
    print 'b=',b
    print 'c=',c
func(1,2,3)

print 80 * '-'

#This is for Keyword paramter
def func1(a='',b='',c=''):
    print 'a=',a
    print 'b=',b
    print 'c=',c
func1(a='First parameter',b='Second parameter',c='First parameter')

print 80 * '-'

#This is for variable list arguments
def func2(*args,**kwargs):
    for x in args:
        print x
    for x, y in kwargs.items():
        print x,y
func2('dog','cat',name='Bob',age=20)        

print 80 * '-'

#This is for global
global a 
a=10
def func3():
    print 'Global variable a=',a
    b= 2
    
func3()

#Lambda#

lista   = [1,2,3] 
listb   = [lambda x:x+10 for x in lista]

    
