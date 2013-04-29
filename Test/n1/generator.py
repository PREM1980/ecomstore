'''
Created on Apr 26, 2013

@author: temp_plakshmanan
'''


def gen(x=10):
    for x in range(x):
        yield x

for x in gen(x=20):
    print x

    