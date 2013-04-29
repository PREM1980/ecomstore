'''
Created on Apr 4, 2013

@author: temp_plakshmanan
'''

import pyodbc
database='CSSC'
tablename="t"
    con = pyodbc.connect(Template("""
        DRIVER={SQL Server};
        SERVER=localhost\SQLEXPRESS;
        DATABASE=$database;
        Trusted_Connection=yes
    """).substitute(locals()))
    cursor = con.cursor()
    cursor.execute("select")
#     con2 = pyodbc.connect(Template("""
#         DRIVER={SQL Server};
#         SERVER=localhost\SQLEXPRESS;
#         DATABASE=$database;
#         Trusted_Connection=yes
#     """).substitute(locals()))
#     cursor2 = con2.cursor()
#     rowlist1 = []
#     rowlist2 = []
#     rowcnt = 0
#     for row in cursor.execute("select * from " + tablename):
#         if rowcnt == 10:
#             break
#         rowlist1.append(row)
#         rowcnt += 1
#     rowcnt = 0
#     for row in cursor2.execute("select * from " + tablename):
#         if rowcnt == 10:
#             break
#         rowlist2.append(row)
#         rowcnt += 1
