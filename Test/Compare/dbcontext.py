import pyodbc as p
 

class DBContext(object):
    def __init__(self,db='',debug=True):
        self.debug = debug
        self.db    = db
        self.conn  = self._getDefaultConn()
        
    
    def _getDefaultConn(self):
        server = 'DC1LAKSHMANANP\SQLEXPRESS'
        database = self.db
        print 'DBCONTEXT db=',self.db
#         database = 'CSSC'
#         database = 'Share'
        connStr = ( r'DRIVER={SQL Server};SERVER=' +
                    server + ';DATABASE=' + database + ';' +
                    'Trusted_Connection=yes'    )
        return p.connect(connStr)
    
    """This method returns a generator object"""
    def execQuery(self,query='',parameters=[]):
        print 'execQUry'
        cur = self.conn.cursor()
        print 'DBCONTEXT cursor=',cur
        
        if self.debug:
            print 'DBCONTEXT execQuery query=',query
            print 'DBCONTEXT execQuery parameters=',parameters
        cur.execute(query,parameters)
        result  = cur.fetchmany(10000)
        cur.close()
        
        if self.debug:
            print 'DBCONTEXT result=',result
        for row in result:
            yield row
    
    def createcur(self):
        return self.conn.cursor()
    
    """This method returns a generator object"""
    def execQuery1(self,cursor,arraysize=10000):
        print 'prem DBCONTEXT execQuery1 cursor=',cursor
        
        while True:
            results = cursor.fetchmany(arraysize)
            print 'DBCONTEXT cursor=',cursor
            if not results:
                break
            for result in results:
                yield result
#             break
                
    def execute(self,query='',parameters=[]):
        cur = self.conn.cursor()
        if self.debug:
            print 'DBCONTEXT execute query=',query
            print 'DBCONTEXT execute parameters=',parameters
            
        cur.execute(query,parameters)
        result  = cur.fetchmany(1000000)
        cur.close()
        if self.debug:
            print 'DBCONTEXT result=',result
        return result
       
        

if __name__ == '__main__':
    x = DBContext(db='CSSC')
    print 'prem'
    x.execQuery(query='select * from sys.columns where object_id = OBJECT_ID(?)', parameters=('Employee1'))
    x.execQuery(query='select * from sys.columns where object_id = OBJECT_ID(?)', parameters=['Employee1'])

