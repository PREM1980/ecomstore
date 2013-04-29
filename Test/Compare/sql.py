'''
Created on Apr 4, 2013

@author: temp_plakshmanan
'''

import pyodbc as p
 
database='CSSC'

server = 'DC1LAKSHMANANP\SQLEXPRESS'
database = 'CSSC'
connStr = ( r'DRIVER={SQL Server};SERVER=' +
                    server + ';DATABASE=' + database + ';' +
                    'Trusted_Connection=yes'    )
con = p.connect(connStr)
cursor = con.cursor()
for row in cursor.execute("select emp_no, emp_fname, emp_lname from employee1"):
        print """Emp_num = {0},         Emp_Fname = {1},           Emp_Lname = {2}""".format(row.emp_no,row.emp_fname,row.emp_lname)
        if getattr(row,'emp_no') == getattr(row,'emp_no'):
            print 'test'
        
         

cursor.close()

cursor = con.cursor()
cursor.execute("select emp_no, emp_fname, emp_lname from employee1")
row = cursor.fetchone()
print row  


