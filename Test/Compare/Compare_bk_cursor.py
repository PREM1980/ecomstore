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

logger = logging.getLogger('Compare')
logger.setLevel(logging.INFO)
logging.basicConfig()
def mynext(it):
    try:
        return next(it)
    except StopIteration:
        raise StopIteration(it)
class Compare(object):
    
    def __init__(self,TableNm1='',TableNm2='',Keyfields=[],excludefields=[],debug=False):
        '''
        Constructor: This Object compares 2 tables and list the differences using key fields 
        '''
        self.conn       = DBContext()
        self.TableNm1   = TableNm1
        self.TableNm2   = TableNm2
        self.Keyfields  = Keyfields
        self.excludefields   = excludefields
        self.num_of_rows_in_table1 = 0  
        self.num_of_rows_in_table2 = 0
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
        self.t1 = 0
        self.t2 = 0
        if not self.Keyfields:
            raise "error"
        print '-'*80
        print '                         Compare Utility Match Report                '
        print '-'*80
    
    def process(self):
        """This step compares """
        self.t1 = datetime.datetime.now()

        self.Table1_Sel_Qry = Query(self.TableNm1,self.Keyfields,self.excludefields).process()
        self.Table2_Sel_Qry = Query(self.TableNm2,self.Keyfields,self.excludefields).process()
        
        if self.debug:
            print "SYSTEM TABLE SELECT QUERY"
            print 'self.Table1_Sel_Qry=',self.Table1_Sel_Qry
            print 'self.Table2_Sel_Qry=',self.Table2_Sel_Qry
            print "SYSTEM TABLE SELECT QUERY----COMPLETE"
        
        query="Select name from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        self.table1_Schema = self.conn.execute(query,self.TableNm1 )
        #print 'self.table1_Schema=',self.table1_Schema
        self.table1_Schema = [str(each[0]).upper() for each in self.table1_Schema]
        query="Select name from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        self.table2_Schema = self.conn.execute(query,self.TableNm2 )
        self.table2_Schema = [str(each[0]).upper() for each in self.table2_Schema]
        self.table1_Schema.remove('_1011_S_PERFORMANCELEVEL_READ')
        self.table1_Schema.remove('_1011_S_PERFORMANCELEVEL_MATH')
        
        if self.debug:
            print "TABLE_SCHEMA"
            print 'self.table1_Schema=',self.table1_Schema
            print 'self.keyfields=',self.Keyfields
            print "TABLE_SCHEMA-----COMPLETE" 
        
        self.table_matchfields = [each for each in self.table1_Schema if each not in self.Keyfields]
        
        if self.debug:
            print 'self.table_matchfields=',self.table_matchfields
        conn1 = DBContext()
        

        table1cur = conn1.createcur()
        table1cur.execute(self.Table1_Sel_Qry)
        
        self.table1_generator = conn1.execQuery1(table1cur)
        
        if self.debug:
            print 'self.table1_generator=',self.table1_generator
        
        try:
            self.table1_row = mynext(self.table1_generator)
            print 'self.table1_row=',self.table1_row
            self.num_of_rows_in_table1 += 1
        except StopIteration:
            self.num_of_rows_in_table1 = 0
        
        conn2 = DBContext()
        
        table2cur = conn2.createcur()
        table2cur.execute(self.Table2_Sel_Qry)
        
        self.table2_generator =  conn2.execQuery1(table2cur)
        
        if self.debug:
            print 'self.table2_generator=',self.table2_generator
        try:
            self.table2_row = mynext(self.table2_generator)
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
#             self._convert_table1_key_fields()
#             self._convert_table2_key_fields()
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
#                         self._convert_table1_key_fields()
                        self.num_of_rows_in_table1 += 1
                        self.table2_row = mynext(self.table2_generator)
#                         self._convert_table2_key_fields()
                        self._check_key_field()
                        self.num_of_rows_in_table2 += 1
                    elif self.table1_key > self.table2_key:
                        if self.debug:
                            print 'key greater'
                        self.compare_ind = 'B'
                        print'1-New row in TABLE2=', self.table2_row_dict
                        self.num_of_new_record_table2 += 1
                        self.table2_row = mynext(self.table2_generator)
#                         self._convert_table2_key_fields()
                        self._check_key_field()
                        self.num_of_rows_in_table2 += 1
                    elif self.table1_key < self.table2_key:
                        if self.debug:
                            print 'key lesser'
                        self.compare_ind = 'C'
                        print'2-New row in TABLE1=', self.table1_row_dict
                        self.num_of_new_record_table1 += 1
                        self.table1_row = mynext(self.table1_generator)
#                         self._convert_table1_key_fields()
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
            
        for key in self.table1_row_dict:
                if key in self.table_matchfields:
#                     print 'key=',key
                    if self.table1_row_dict[key] == self.table2_row_dict[key]:
                        pass
                    else:
                        if len(self.error_count_lists) == 0:
                            self.error_count[key] = 1 
                            self.error_count_lists.append(self.error_count)
                            self.error_dict['ROWNUM']  = self.num_of_rows_in_table1
                            self.error_dict['FIELD']   = key
                            self.error_dict['VALUE-1'] = self.table1_row_dict[key]
                            self.error_dict['VALUE-2'] = self.table2_row_dict[key]
                            self.error_lists.append(self.error_dict)
                        else:
                            for each in self.error_count_lists:
                                if key in each:
                                    if self.error_count[key] == 50:
                                        break
                                    self.error_count[key] = self.error_count[key] + 1
                                else:
                                    self.error_count[key] = 1 
                                    self.error_count_lists.append(self.error_count)
                                self.error_dict['ROWNUM']  = self.num_of_rows_in_table1
                                self.error_dict['FIELD']   = key
                                self.error_dict['VALUE-1'] = self.table1_row_dict[key]
                                self.error_dict['VALUE-2'] = self.table2_row_dict[key]
                                self.error_lists.append(self.error_dict)
#                         print 'self.error_lists=', self.error_lists
#                         print 'self.error_count=',self.error_count_lists
#                         print 'Row Number              =',self.num_of_rows_in_table1
#                         print 'Field Name not matching =',key
#                         print 'Table-1 Field value     =',self.table1_row_dict[key]
#                         print 'Table-2 Field value     =',self.table2_row_dict[key] 
        
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
                    #self._convert_table2_key_fields()
                    self.table2_row_dict = dict((el1,el2) for el1, el2 in zip(self.table2_Schema,list(self.table2_row)))
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
                    #self._convert_table1_key_fields()
                    self.table1_row_dict = dict((el1,el2) for el1, el2 in zip(self.table1_Schema,list(self.table1_row)))
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
        self.table1_row_dict = dict((el1,el2) for el1, el2 in zip(self.table1_Schema,list(self.table1_row)))
        self.table2_row_dict = dict((el1,el2) for el1, el2 in zip(self.table2_Schema,list(self.table2_row)))
        if self.debug:
            print 'self.table1_row_dict=',self.table1_row_dict
            print 'self.table2_row_dict=',self.table2_row_dict
#             print 'self.keyfields',self.Keyfields
        for x in self.Keyfields:
            if self.debug:
                print 'self.table1_row_dict[x]=',self.table1_row_dict[x]
                print 'self.table1_row_dict[x]=',self.table2_row_dict[x]
            if self.table1_row_dict[x] == self.table2_row_dict[x]:
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
                
            
#     def _convert_table1_key_fields(self):
#         self.table1_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_Schema,list(self.table1_row)))
#         if self.debug:
#             print 'table1_row_dict=',self.table1_row_dict
#         for x,y in zip(range(len(self.Keyfields)),self.Keyfields):
#             if x==0:
#                 self.table1_key = '' + str(self.table1_row_dict[y])
#             else:
#                 self.table1_key = self.table1_key + str(self.table1_row_dict[y])
#             
#     def _convert_table2_key_fields(self):
#         self.table2_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_Schema,list(self.table2_row)))
#         for x,y in zip(range(len(self.Keyfields)),self.Keyfields):
#             if x==0:
#                 self.table2_key = '' + str(self.table2_row_dict[y])
#             else:
#                 self.table2_key = self.table2_key + str(self.table2_row_dict[y])
            
    def _printreport(self):
        print '-'*80
        print 'Number of records in TABLE1=',self.num_of_rows_in_table1   
        print 'Number of records in TABLE2=',self.num_of_rows_in_table2
        print """Number of records with MATCHED KEYS=""",self.num_of_matched_keys
        print "Number of new records in TABLE-1=",self.num_of_new_record_table1
        print "Number of new records in TABLE-2=",self.num_of_new_record_table2
        print "Number of variables compared=",len(self.table_matchfields)
        print "error_lists=",self.error_count_lists
        print "error_count=",self.error_lists
        print '-'*80
        self.t2 = datetime.datetime.now()
        print 'Time taken=',self.t2-self.t1
#         line1 = 'Number of records in TABLE1=' + str(self.num_of_rows_in_table1)
#         line2 = 'Number of records in TABLE2=' + str(self.num_of_rows_in_table2)
#         line3 = 'Number of records with MATCHED KEYS='+ str(self.num_of_matched_keys)
#         line4 = "Number of new records in TABLE-1=" + str(self.num_of_new_record_table1)
#         line5 = "Number of new records in TABLE-2=" + str(self.num_of_new_record_table2)
#         line6 = "Number of variables compared=" + str(len(self.table_matchfields))
# #         print self.error_lists
#         c = canvas.Canvas("Compare report")
#         
# #         width, height = letter
#         c.drawString(80, 800, line1)
#         c.drawString(80, 785, line2)
#         c.drawString(80, 770, line3)
#         c.drawString(80, 755, line4)
#         c.drawString(80, 740, line5)
#         c.drawString(80, 725, line6)
# #        c.showPage()
#         c.save()
#         
        print "Compare ran"

if __name__ == '__main__':
#     x = Compare(TableNm1='Employee1',TableNm2='Employee2',Keyfields=['EMP_NO'])
    x = Compare(TableNm1='dbo.read_gr04_1',TableNm2='dbo.read_gr04_2',Keyfields=['SSID'])
    x.process()



