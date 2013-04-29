'''
Created on Mar 28, 2013

@author: temp_plakshmanan
'''

class MyClass(object):
    '''
    classdocs
    '''


    def __init__(self,x,y):
        '''
        Constructor
        '''
        self.x = x
        self.y = y 
        lista = [1,2,3]
        self.test()
        
        
    def test(self):
        print 'Im in test'
        
        
if __name__ == '__main__':
    pass        