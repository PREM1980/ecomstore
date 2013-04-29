'''
Created on Mar 29, 2013

@author: temp_plakshmanan
'''
from dbcontext import DBContext
from Query import Query
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, A4
import logging
import datetime
from pprint import pprint

logger = logging.getLogger('Compare')
logger.setLevel(logging.INFO)
logging.basicConfig()
def mynext(it):
    try:
        return next(it)
    except StopIteration:
        raise StopIteration(it)
class Compare(object):
    
    def __init__(self,TableNm1='',TableNm2='',Keyfields=[],excludefields=[],db='',num_of_errors=0,tolerance=0,debug=False):
        '''
        Constructor: This Object compares 2 tables and list the differences using key fields 
        '''
        self.db    = db
        self.incr  = 0 
        self.num_of_errors = num_of_errors
        self.tolerance = tolerance
        self.conn       = DBContext(db=self.db)
        self.TableNm1   = TableNm1
        self.TableNm2   = TableNm2
        self.Keyfields  = Keyfields
        self.excludefields   = excludefields
        self.num_of_rows_in_table1 = 0  
        self.num_of_rows_in_table2 = 0
        self.num_of_rows_in_table1 = 0
        self.table1_key = '' 
        self.table2_key = ''
        self.table1_row = '' 
        self.table2_row = ''
        self.exhaust_table_1 = 'N'
        self.exhaust_table_2 = 'N'
        self.compare_ind = ''
        self.debug = debug
        self.table1_row_dict = ''
        self.table2_row_dict = ''
        self.num_of_matched_keys = 0
        self.num_of_new_record_table1 = 0 
        self.num_of_new_record_table2 = 0
        self.error_dict = {}
        self.error_count = {}
        self.error_lists = []
        self.error_count_lists = []
        self.key_values_list = []
        self.table1_Schema_type = {}
        self.table2_Schema_type = {}
        self.t1 = datetime.datetime.now()
        if not self.Keyfields:
            raise "error"
        print '-'*80
        print '                         Compare Utility Match Report                '
        print '-'*80
    
    def process(self):
        """This step compares """
        self.Table1_Sel_Qry, self.Table2_Sel_Qry,self.table_matchfields_with_key = Query(self.TableNm1,self.TableNm2,self.Keyfields,self.excludefields,self.db).process()
        if self.debug:
            print "SYSTEM TABLE SELECT QUERY"
            print 'self.Table1_Sel_Qry=',self.Table1_Sel_Qry
            print 'self.Table2_Sel_Qry=',self.Table2_Sel_Qry
            print "SYSTEM TABLE SELECT QUERY----COMPLETE"
        
        
        query="Select name,TYPE_NAME(system_type_id),max_length from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        self.table1_Schema      = self.conn.execute(query,self.TableNm1 )
        self.table1_Schema_type = dict( (str(each[0]).upper() ,[str(each[1]).upper(), each[2] ] ) for each in self.table1_Schema)
        self.table1_Schema      = [str(each[0]).upper() for each in self.table1_Schema]
        
        query="Select name,TYPE_NAME(system_type_id),max_length from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        self.table2_Schema      = self.conn.execute(query,self.TableNm2 )
        self.table2_Schema_type = dict( (str(each[0]).upper() ,[str(each[1]).upper(), each[2] ] ) for each in self.table2_Schema)
        self.table2_Schema      = [str(each[0]).upper() for each in self.table2_Schema]

        self.table1_not_in_table2_fields = [each for each in self.table1_Schema if each not in self.table_matchfields_with_key]
        self.table2_not_in_table1_fields = [each for each in self.table2_Schema if each not in self.table_matchfields_with_key]
        self.table_matchfields = [each for each in self.table_matchfields_with_key if each not in self.Keyfields]
        
        if self.debug:
            print "TABLE_SCHEMA"
            print 'self.table1_Schema=',self.table1_Schema
            print 'self.table2_Schema=',self.table2_Schema
            print 'self.keyfields=',self.Keyfields
            print 'self.table_matchfields=',self.table_matchfields
            print 'self.table1_not_in_table2_fields=',self.table1_not_in_table2_fields
            print 'self.table2_not_in_table1_fields=',self.table2_not_in_table1_fields
            print "TABLE_SCHEMA-----COMPLETE"
            
        conn1 = DBContext(db=self.db)
        table1cur = conn1.createcur()
        table1cur.execute(self.Table1_Sel_Qry)
        self.table1_generator = conn1.execQuery1(table1cur)
        
        
        
        try:
            self.table1_row = mynext(self.table1_generator)
            if self.debug:
                print 'self.table1_generator=',self.table1_generator
                print 'self.table1_row=',self.table1_row
            self.num_of_rows_in_table1 += 1
        except StopIteration:
            self.num_of_rows_in_table1 = 0
        
        conn2 = DBContext(db=self.db)
        table2cur = conn2.createcur()
        table2cur.execute(self.Table2_Sel_Qry)
        self.table2_generator =  conn2.execQuery1(table2cur)
        
        
        try:
            self.table2_row = mynext(self.table2_generator)
            if self.debug:
                print 'self.table2_generator=',self.table2_generator
                print 'self.table2_row=',self.table2_row
            self.num_of_rows_in_table2 += 1
        except StopIteration:
            self.num_of_rows_in_table2 = 0
        
        if self.debug:
            print 'self.num_of_rows_in_table1=',self.num_of_rows_in_table1
            print 'self.num_of_rows_in_table2=',self.num_of_rows_in_table2
        
        if  (self.num_of_rows_in_table1 == 0 and
            self.num_of_rows_in_table2 == 0):
            self._printreport()
        elif (self.num_of_rows_in_table1 == 0 and 
             self.num_of_rows_in_table2 <> 0):
            self.exhaust_table_1 = 'N'
            self.exhaust_table_2 = 'Y'
            self._process_addn_rows()
            self._printreport()
        elif (self.num_of_rows_in_table1 <> 0 and 
             self.num_of_rows_in_table2 == 0):
            self.exhaust_table_1 = 'Y'
            self.exhaust_table_2 = 'N'
            self._process_addn_rows()
            self._printreport()
        else:
            self._check_key_field()
            self._comparekeys()
            self._process_addn_rows()
            self._printreport()
    
    def _comparekeys(self):
        end_of_table_1 = 'N'
        end_of_table_2 = 'N'
        while end_of_table_1 =='N' and end_of_table_2 == 'N':
                if self.debug:
                    print 'Compare start'
                    print 'self.table1_key=',self.table1_key
                    print 'self.table2_key=',self.table2_key
                try:
                    if self.table1_key == self.table2_key:
                        if self.debug:
                            print 'same'
                        self.compare_ind = 'A'
                        self._comparefields()
                        self.table1_row = mynext(self.table1_generator)
                        self.num_of_rows_in_table1 += 1
                        self.table2_row = mynext(self.table2_generator)
                        self._check_key_field()
                        self.num_of_rows_in_table2 += 1
                    elif self.table1_key > self.table2_key:
                        if self.debug:
                            print 'key greater'
                        self.compare_ind = 'B'
                        print'1-New row in TABLE2=', self.table2_row_dict
                        self.num_of_new_record_table2 += 1
                        self.table2_row = mynext(self.table2_generator)
                        self._check_key_field()
                        self.num_of_rows_in_table2 += 1
                    elif self.table1_key < self.table2_key:
                        if self.debug:
                            print 'key lesser'
                            print'2-New row in TABLE1=', self.table1_row_dict
                        self.compare_ind = 'C'
                        self.num_of_new_record_table1 += 1
                        self.table1_row = mynext(self.table1_generator)
                        self._check_key_field()
                        self.num_of_rows_in_table1 += 1
                except StopIteration as e:
                    if self.debug:
                        print 'self.table1_generator=',self.table1_generator
                        print 'self.table2_generator=',self.table2_generator
                        print 'ERROR                =',e.args[0]
                        print 'self.compare_ind=',self.compare_ind
                    if self.table1_generator == e.args[0]:
                        if self.debug:
                            print 'equal'
                        if self.compare_ind == 'A':
                            self.exhaust_table_2 = 'Y'
                            self.exhaust_table_1 = 'N'
                        elif self.compare_ind == 'B':
                            self.exhaust_table_2 = 'Y'
                            self.exhaust_table_1 = 'N'
                        elif self.compare_ind == 'C':
                            self.exhaust_table_1 = 'Y'
                            self.exhaust_table_2 = 'N'
                            if self.debug:
                                print'3-New row in TABLE2=', self.table2_row_dict
                            self.num_of_new_record_table2 += 1
                    elif self.table2_generator == e.args[0]:
                        if self.debug:
                            print 'not equal'
                        if self.compare_ind == 'A':
                            self.exhaust_table_2 = 'N'
                            self.exhaust_table_1 = 'Y'
                        elif self.compare_ind == 'B':
                            self.exhaust_table_2 = 'Y'
                            self.exhaust_table_1 = 'N'
                            if self.debug:
                                print'4-New row in TABLE1=', self.table1_row_dict
                            self.num_of_new_record_table1 += 1
                        elif self.compare_ind == 'C':
                            self.exhaust_table_1 = 'Y'
                            self.exhaust_table_2 = 'N'
                    if self.debug:                            
                        print 'IterError'
                    end_of_table_1 = end_of_table_2 = 'Y' 
                    
    def _comparefields(self):
        self.num_of_matched_keys +=1 
        if self.debug:
            print 'Compare fields'
            print 'table1_row_dict=',self.table1_row_dict
            print 'table2_row_dict=',self.table2_row_dict
            
            
#         for key in self.table1_row_dict:
#             if key in self.table_matchfields:
#                 if self.table1_row_dict[key] == self.table2_row_dict[key]:
#                     if self.table1_Schema_type[key] <> self.table2_Schema_type[key]:
#                         self._write_error_lists(key)
#                 else:
#                     self._write_error_lists(key)
        for key in self.table1_row_dict:
            if key in self.table_matchfields:
                if self.debug:
                    print 'self.table1_Schema_type[key][0]=',self.table1_Schema_type[key][0]
                    print 'self.table2_Schema_type[key][0]=',self.table2_Schema_type[key][0]
                    print 'self.table1_Schema_type[key][1]=',self.table1_Schema_type[key][1]
                    print 'self.table2_Schema_type[key][1]=',self.table2_Schema_type[key][1]
                    print 'self.table1_row_dict[key]=', self.table1_row_dict[key]
                    print 'self.table2_row_dict[key]=', self.table2_row_dict[key]
                if self.table1_Schema_type[key][0] == self.table2_Schema_type[key][0]: # Checks for the type of the field
                    if self.table1_row_dict[key] == self.table2_row_dict[key]:
                        pass
                    else:
                        compare, numeric = self._check_types(key)
                        if numeric:
#                             if self.table1_row_dict[key] >= self.table2_row_dict[key]:
#                                 result = self.table1_row_dict[key] - self.table2_row_dict[key]
#                             else:
#                                 result = self.table2_row_dict[key] - self.table1_row_dict[key]
                                
                            result = abs(self.table1_row_dict[key] - self.table2_row_dict[key])  / max([abs(self.table1_row_dict[key]),abs(self.table2_row_dict[key])])    
                            if result >= self.tolerance:
                                self._write_error_lists(key)
                        else:
                            self._write_error_lists(key)
#                         if self.table1_Schema_type[key][1] == self.table2_Schema_type[key][1]: #Checks for the length of the field
#                             pass
#                         else:
#                             self._write_error_lists(key)
                else:
                    compare, numeric = self._check_types(key)
                    if compare:
                        if self.table1_row_dict[key] == self.table2_row_dict[key]:
                            pass
                        else:
                            if numeric:
#                                 if self.table1_row_dict[key] >= self.table2_row_dict[key]:
#                                     result = self.table1_row_dict[key] - self.table2_row_dict[key]
#                                 else:
#                                     result = self.table2_row_dict[key] - self.table1_row_dict[key]
                                
                                result = abs(self.table2_row_dict[key] - self.table1_row_dict[key]) / max([self.table2_row_dict[key],self.table1_row_dict[key]])
                                if result >= self.tolerance:
                                    self._write_error_lists(key)
                            else:
                                self._write_error_lists(key)
        self.key_values_list = []
   
    def _tolerance_check(self,key):
        pass
    
    def _check_types(self,key):
            compare = False
            numeric = False
            if ((self.table1_Schema_type[key][0] == 'VARCHAR' and self.table2_Schema_type[key][0] == 'CHAR') or         \
                (self.table1_Schema_type[key][0] == 'CHAR' and self.table2_Schema_type[key][1] == 'VARCHAR')): 
                numeric = False
                compare = True        
            elif ((self.table1_Schema_type[key][0] == 'DECIMAL' and self.table2_Schema_type[key][0] == 'FLOAT') or       \
                  (self.table1_Schema_type[key][0] == 'FLOAT' and self.table2_Schema_type[key][1] == 'DECIMAL') or
                  (self.table1_Schema_type[key][0] == 'DECIMAL' and self.table2_Schema_type[key][0] == 'DECIMAL') or
                  (self.table1_Schema_type[key][0] == 'FLOAT' and self.table2_Schema_type[key][0] == 'FLOAT')):
                numeric = True
                compare = True
            return compare, numeric
        
        
    def _write_error_lists(self,key):
        if self.debug:
            print '_write_error_lists='
            
        key_present = False
        key_not_present = False
        if len(self.error_count_lists) == 0:
            self.error_count[key] = 1
            self.error_count_lists.append(self.error_count)
        else:
            for each in self.error_count_lists:
                if key in each:
                    key_present = True
                    break
                else:
                    key_not_present = True
        if self.debug:
            print 'key_present=',     key_present
            print 'key_not_present=', key_not_present 
        
        if key_present == True:self.error_count[key] = self.error_count[key] + 1
        
        if key_not_present == True:
            self.error_count[key] = 1

        if self.error_count[key] < self.num_of_errors:
            self._write_error_dict(key)
            

        
    def _write_error_dict(self,key):
        self.error_dict['KEY']  = self.key_values_list
        self.error_dict['FIELD']   = key
        self.error_dict['VALUE-1'] = self.table1_row_dict[key]
        self.error_dict['VALUE-2'] = self.table2_row_dict[key]
        self.error_lists.append(self.error_dict)
        self.error_dict = {}
         
        
    def _process_addn_rows(self):
        if self.debug:
            print '_process_Addn_rows'
            print 'self.exhaust_table_2=',self.exhaust_table_2
            print 'self.exhaust_table_1=',self.exhaust_table_1
        end_of_table_1 = end_of_table_2 = 'N'
        if self.exhaust_table_2 == 'Y':
            while end_of_table_2 == 'N':
                try:
                    self.table2_row = mynext(self.table2_generator)
                    self.table2_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields_wth_key,list(self.table2_row)))
                    print'New row in TABLE2=', self.table2_row_dict
                    self.num_of_new_record_table2 += 1
                    self.num_of_rows_in_table2 += 1
                except StopIteration as e:
                    if self.debug:
                        print 'ERROR=',e.args
                        print 'self.table1_generator=',self.table1_generator
                    if self.table1_generator == e.args:
                        if self.debug:
                            print 'equal'
                    else:
                        if self.debug:
                            print 'not equal'
                    end_of_table_2 = 'Y'
        
        if self.exhaust_table_1 == 'Y':
            while end_of_table_1 == 'N':
                try:
                    self.table1_row = mynext(self.table1_generator)
                    self.table1_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields,list(self.table1_row)))
                    print'New row in TABLE1=', self.table1_row_dict
                    self.num_of_new_record_table1 += 1
                    self.num_of_rows_in_table1 += 1
                except StopIteration as e:
                    if self.debug:
                        print 'ERROR=',e.args
                        print 'self.table1_generator=',self.table1_generator
                    end_of_table_1 = 'Y'
        
        self.exhaust_table_1 = 'N'
        self.exhaust_table_2 = 'N'
                
    def _check_key_field(self):
        self.table1_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields_with_key,list(self.table1_row)))
        self.table2_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields_with_key,list(self.table2_row)))
        if self.debug:
            print 'self.table1_Schema=', self.table1_Schema
            print 'self.table2_Schema=', self.table2_Schema
            print 'self.table1_row_dict=',self.table1_row_dict
            print 'self.table2_row_dict=',self.table2_row_dict
        for x in self.Keyfields:
            if self.debug:
                print 'self.table1_row_dict[x]=',self.table1_row_dict[x]
                print 'self.table1_row_dict[x]=',self.table2_row_dict[x]
            if self.table1_row_dict[x] == self.table2_row_dict[x]:
                self.key_values_list.append(self.table1_row_dict[x])
                self.table1_key = 1
                self.table2_key = 1
            elif self.table1_row_dict[x] > self.table2_row_dict[x]:
                self.table1_key = 2
                self.table2_key = 1
                
                break
            elif self.table1_row_dict[x] < self.table2_row_dict[x]:
                self.table1_key = 1
                self.table2_key = 2
                
                break
                
    def _printreport(self):
        print '-'*80 + 'Error Report' + '-'*80 
        print "error_count=",self.error_count_lists
        self.TableNm1 = self.TableNm1.upper()
        self.TableNm2 = self.TableNm2.upper()
        for x in self.error_count_lists:
            pprint(x)
        
        print "error_lists=",self.error_lists
        num_of_error_keys =[]
        from operator import itemgetter
        error_lists = sorted(self.error_lists,key=itemgetter('FIELD'))
        for x in error_lists:
            print x
            num_of_error_keys.append(x['KEY'][0])
        print 'Number of Observations in TABLE1=',self.num_of_rows_in_table1   
        print 'Number of Observations in TABLE2=',self.num_of_rows_in_table2
        print "Number of Observations with MATCHED KEYS=",self.num_of_matched_keys
        print "Number of Observations with compared variables unequal=", len(set(num_of_error_keys))
        print "Number of Observations with compared variables EQUAL=", self.num_of_matched_keys - len(set(num_of_error_keys))
        print "Number of new Observations in TABLE-1=",self.num_of_new_record_table1
        print "Number of new Observations in TABLE-2=",self.num_of_new_record_table2
        print "Number of variables compared=",len(self.table_matchfields)
        print "Number of Variables Compared with All Observations Equal=",len(self.table_matchfields) - len(self.error_count_lists[0].keys())
        print "Number of Variables Compared with Some Observations Unequal=",len(self.error_count_lists[0].keys())
        print "Total number of values which compare unequal=",len(self.error_lists)
        print "Number of variables in Table-1 and not in Table-2=",self.table1_not_in_table2_fields
        print "Number of variables in Table-2 and not in Table-1=",self.table2_not_in_table1_fields
        schema_types = []
        for x in self.table1_Schema_type:
            schema_types.append(self.table1_Schema_type[x][0])
        
        for x in self.table2_Schema_type:
            schema_types.append(self.table2_Schema_type[x][0])
        
        print "Types of variables compared=", set(schema_types)    
        
        self.t2 = datetime.datetime.now()
        print 'Time taken=',self.t2-self.t1
        print '-'*80
        
        
        line1  = 'Number of Observations in {0} = {1}'.format(self.TableNm1,str(self.num_of_rows_in_table1))   
        line2  = 'Number of Observations in {0} = {1}'.format(self.TableNm2,str(self.num_of_rows_in_table2))
        line3  = "Number of variables in common = {0}".format(str(len(self.table_matchfields)))
        line4  =  "Number of variables in {0} and not in {1} = {2} ".format(self.TableNm1,self.TableNm2,str(len(self.table1_not_in_table2_fields)))
        line5  = "Number of variables in {0} and not in {1} = {2}".format(self.TableNm2,self.TableNm1, str(len(self.table2_not_in_table1_fields)))
        line6  = "Number of ID variables=" + str(len(self.Keyfields))
        
        line7  = "Number of Observations with MATCHED KEYS = " + str(self.num_of_matched_keys)
        line8  = "Number of Observations with compared variables unequal = " + str(len(set(num_of_error_keys)))
        line9  = "Number of Observations with compared variables EQUAL = " + str(self.num_of_matched_keys - len(set(num_of_error_keys)))
        line10  = "Number of new Observations in TABLE-1 = " + str(self.num_of_new_record_table1)
        line11  = "Number of new Observations in TABLE-2 = " + str(self.num_of_new_record_table2)
        line12  = "Number of variables compared = " + str(len(self.table_matchfields))
        line13  = "Number of Variables Compared with All Observations Equal = " +str(len(self.table_matchfields) - len(self.error_count_lists[0].keys()))
        line14 = "Number of Variables Compared with Some Observations Unequal = " + str(len(self.error_count_lists[0].keys()))
        line15 = "Total number of values which compare unequal = " + str(len(self.error_lists))
        
#         print self.error_lists
        c = canvas.Canvas("Compare report",pagesize=letter)
        
        width, height = letter
        print 'width=',width
        print 'height=',height
        c.setFont("Helvetica", 12)
        startval = 750
        linediff = 12
        self.incr = 2
        
        c.drawString(200, startval,               "The COMPARE Procedure Report")
        t1=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.drawString(400, startval - linediff,  "Run time = {0}".format(t1))
        c.setFont("Helvetica", 8)
        c.drawString(400, startval - (self._incr_val() *linediff),  "Num of output errors = {0}".format(self.num_of_errors))
        c.drawString(400, startval - (self._incr_val() *linediff),  "Tolerance = {0}".format(self.num_of_errors))
        c.setFont("Helvetica", 10)
        
        c.drawString(220, startval - (self._incr_val() * linediff),  "Data set summary")
        c.setFont("Helvetica", 8)
        
        c.drawString(80, startval  - (self._incr_val() * linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
        c.drawString(320, startval - (self.incr * linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
        c.drawString(80, startval  - (self._incr_val() * linediff),  line1)
        c.drawString(80, startval  - (self._incr_val() * linediff),  line2)
        c.drawString(80, startval  - (self._incr_val() * linediff),  "")
        c.setFont("Helvetica", 10)
        c.drawString(220, startval - (self._incr_val() * linediff),  "Variables summary")
        c.setFont("Helvetica", 8)
        c.drawString(80, startval - (self._incr_val() * linediff),  line3)
        c.drawString(80, startval - (self._incr_val() * linediff),  line4)
        c.drawString(80, startval - (self._incr_val() * linediff),  line5)
        c.drawString(80, startval - (self._incr_val() * linediff),  line6)
        
        c.drawString(80, startval - (self._incr_val() * linediff),  "")
        c.setFont("Helvetica", 10)
        c.drawString(160, startval - (self._incr_val() * linediff), "Number of variables in {0} and not in {1}".format(self.TableNm1,self.TableNm2))
        
        c.setFont("Helvetica", 10)
        c.drawString(80,startval  - (self._incr_val() * linediff), "FIELD")
        c.drawString(300,startval - (self.incr        * linediff), "TYPE")
        c.drawString(400,startval - (self.incr        * linediff), "LENGTH")
        
        c.setFont("Helvetica", 8)
        
        for x in range(len(self.table1_not_in_table2_fields)):
            if self.incr * linediff > 700:
                c.showPage()
                c.setFont("Helvetica", 10)
                c.drawString(80, startval  - (self._incr_val() * linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
                c.drawString(320, startval - (self.incr * linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
                c.drawString(400, startval - linediff,  "Run time = {0}".format(t1))
                c.drawString(80, startval - (self._incr_val() * linediff),  "")
                c.setFont("Helvetica", 8)
            c.drawString(80,startval - (self._incr_val() * linediff), self.table1_not_in_table2_fields[x] )
            print 'self.table1_not_in_table2_fields[x]=',self.table1_not_in_table2_fields[x]
            key=self.table1_not_in_table2_fields[x]
            c.drawString(300,startval - (self.incr * linediff), str(self.table1_Schema_type[key][0]))
            c.drawString(400,startval - (self.incr * linediff), str(self.table1_Schema_type[key][1]))
            
        c.drawString(80, startval - (self._incr_val() * linediff),  "")
        c.setFont("Helvetica", 10)
        c.drawString(160, startval - (self._incr_val() * linediff), "Number of variables in {0} and not in {1}".format(self.TableNm2,self.TableNm1))
        
        c.setFont("Helvetica", 10)
        c.drawString(80,startval  - (self._incr_val() * linediff), "FIELD")
        c.drawString(300,startval - (self.incr        * linediff), "TYPE")
        c.drawString(400,startval - (self.incr        * linediff), "LENGTH")
        c.setFont("Helvetica", 8)
       
        for x in range(len(self.table2_not_in_table1_fields)):
            if self.incr * linediff > 700:
                self.incr = 2
                c.showPage()
                c.setFont("Helvetica", 10)
                c.drawString(80, startval  - (self._incr_val() * linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
                c.drawString(320, startval - (self.incr * linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
                c.drawString(400, startval - linediff,  "Run time = {0}".format(t1))
                c.drawString(80, startval - (self._incr_val() * linediff),  "")
                
                c.setFont("Helvetica", 8)
            c.drawString(80,startval - (self._incr_val() * linediff), self.table2_not_in_table1_fields[x] )
            key=self.table2_not_in_table1_fields[x]
            c.drawString(300,startval - (self.incr * linediff), str(self.table2_Schema_type[key][0]))
            c.drawString(400,startval - (self.incr * linediff), str(self.table2_Schema_type[key][1]))
             
        c.showPage()
        self.incr = 2
        c.setFont("Helvetica", 10)
        c.drawString(80, startval  - (self._incr_val() * linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
        c.drawString(320, startval - (self.incr * linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
        c.drawString(400, startval - linediff,  "Run time = {0}".format(t1))
        c.drawString(80, startval - (self._incr_val() * linediff),  "")
        
        
        c.setFont("Helvetica", 10)
        
        c.drawString(80, startval - (self._incr_val() * linediff), "Observation Summary")
        
        c.setFont("Helvetica", 8)
         
        c.drawString(80, startval - (self._incr_val() * linediff), line7)
        c.drawString(80, startval - (self._incr_val() * linediff), line8)
        c.drawString(80, startval - (self._incr_val() * linediff), line9)
        c.drawString(80, startval - (self._incr_val() * linediff), line10)
        c.drawString(80, startval - (self._incr_val() * linediff), line11)
        c.drawString(80, startval - (self._incr_val() * linediff), line12)
        c.drawString(80, startval - (self._incr_val() * linediff), line13)
        c.drawString(80, startval - (self._incr_val() * linediff), line14)
        c.drawString(80, startval - (self._incr_val() * linediff), line15)
        c.drawString(80, startval - (self._incr_val() * linediff),  "")
        c.setFont("Helvetica", 10)
        c.drawString(220, startval - (self._incr_val() * linediff),  "Variables with unequal values")
        c.drawString(80, startval - (self._incr_val() * linediff),  "")
        c.drawString(80,startval -  (self._incr_val() * linediff), "FIELD NAME")
        c.drawString(400,startval - (self.incr        * linediff), "COUNT")
        
        c.setFont("Helvetica", 8)
        for x in self.error_count_lists:
            for eachitem in x:
                if self.incr * linediff > 700:
                    self.incr = 2
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    c.drawString(80, startval  - (self._incr_val() * linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
                    c.drawString(320, startval - (self.incr * linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
                    c.drawString(400, startval - linediff,  "Run time = {0}".format(t1))
                    c.setFont("Helvetica", 8)
                    c.drawString(80, startval - (self._incr_val() * linediff),  "")
                c.drawString(80,startval -  (self._incr_val() * linediff), eachitem)
                c.drawString(400,startval - (self.incr        * linediff), str(x[eachitem]))
                
        c.drawString(80, startval - (self._incr_val() * linediff),  "")
        c.setFont("Helvetica", 10)
        c.drawString(220, startval - (self._incr_val() * linediff),  "Value comparison Results for variables")
        c.drawString(80, startval - (self._incr_val() * linediff),  "")
                
        c.setFont("Helvetica", 10)
        
        c.drawString(80,startval  - (self._incr_val() * linediff), "Key")
        c.drawString(400,startval - (self.incr        * linediff), "Compare")
        c.drawString(240,startval - (self.incr        * linediff), "Base")
        #c.drawString(500,startval - (self.incr * linediff), "Field")
        c.setFont("Helvetica", 8)
        for x in range(len(error_lists)):
                print 'self.incr * linediff=',self.incr * linediff
                if self.incr * linediff > 700:
                    self.incr = 2
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    c.drawString(80, startval  - (self._incr_val() * linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
                    c.drawString(320, startval - (self.incr * linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
                    c.drawString(400, startval - linediff,  "Run time = {0}".format(t1))
                    c.drawString(80, startval - (self._incr_val() * linediff),  "")
                    c.setFont("Helvetica", 8)
                if x == 0:
                    field_name = error_lists[x]['FIELD']
                    c.drawString(240, startval - (self._incr_val() * linediff),  error_lists[x]['FIELD'])
                    c.drawString(400, startval - (self.incr * linediff),  error_lists[x]['FIELD'])
                else:
                    if field_name <> error_lists[x]['FIELD']:
                        c.drawString(80, startval - (self._incr_val() * linediff),  "")
                        
                        c.setFont("Helvetica", 10)
                        c.drawString(80,startval  - (self._incr_val() * linediff), "Key")
                        c.drawString(400,startval - (self.incr        * linediff), "Compare")
                        c.drawString(240,startval - (self.incr        * linediff), "Base")
                        c.setFont("Helvetica", 8)
                        c.drawString(240, startval - (self._incr_val() * linediff),  error_lists[x]['FIELD'])
                        c.drawString(400, startval - (self.incr * linediff),  error_lists[x]['FIELD'])
                        
                        
                key = ','.join(error_lists[x]['KEY'])                    
                c.drawString(80, startval - (self._incr_val() * linediff), key)        
                c.drawString(240,startval - (self.incr        * linediff), str(error_lists[x]['VALUE-1']))
                c.drawString(400,startval - (self.incr        * linediff), str(error_lists[x]['VALUE-2']))
                #c.drawString(500,startval -  (self.incr * linediff), error_lists[x]['FIELD'])
                
                field_name = error_lists[x]['FIELD']
                
        c.save()
         
        print "Compare ran"
    
            
    def _incr_val(self):
            self.incr += 1
            return self.incr 

if __name__ == '__main__':
    #x = Compare(TableNm1='Employee1',TableNm2='Employee2',Keyfields=['EMP_NO','EMP_FNAME'],db='CSSC',num_of_errors=50)
    x = Compare(TableNm1='dbo.compare1',TableNm2='dbo.compare2',Keyfields=['SSID'],db='Share',num_of_errors=50,tolerance=0.000001)
#     x = Compare(TableNm1='dbo.read_gr04_1',TableNm2='dbo.read_gr04_2',Keyfields=['SSID'])
    x.process()






