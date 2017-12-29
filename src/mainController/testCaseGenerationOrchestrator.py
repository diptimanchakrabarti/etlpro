'''
Created on Nov 9, 2017

@author: IBM
'''

import os
import pandas as pd
import yaml
from pandas import ExcelWriter
from pandas import ExcelFile
from baseUtils import testCaseGenerationLogger
from baseUtils import testCaseGenerationAuditor
from baseUtils import testCaseGenerationErrorHandler
from mainController import testCaseGenerationValidator
from writerUtil import testCaseGenerationXLSWriter
from mainController import testCaseGenerationConfig

from textUtils import testCaseGenerationBaseParser
from textUtils import testCaseGenerationTestCaseClassifer


class testCaseGenerationOrchestartor(object):

#     orchestratorError=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
    
    def __init__(self):
        logger =   testCaseGenerationLogger.testCaseGenerationLogger()
        self.log = logger.getLogger(self.__class__.__name__)
        #self.log.info('This is logger for testCaseGenerationOrchestrator:init method')
        self.errhandler=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
        reload(testCaseGenerationConfig)


    def reader_orchestrator(self,fname):
        
        readXlsAuditor=testCaseGenerationAuditor.testCaseGenerationAuditor()
        #readXlsError=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
        readXlsValidator = testCaseGenerationValidator.testCaseGenerationValidator()
        baseParser = testCaseGenerationBaseParser.testCaseGenerationBaseParser()
        xlswriter = testCaseGenerationXLSWriter.testCaseGenerationXLSWriter()
        classifier=testCaseGenerationTestCaseClassifer.testCaseGenerationTestCaseClassifier()
        self.log.info('*****Initializing..........')
        self.log.info('*****START=Mapping File Validation...')
        if readXlsValidator.validate_workbook(fname):
            try:
                self.log.info('*****END=Mapping file validation.')
                df = pd.read_excel(fname, sheet_name='Attribute Mapping', skiprows=1,na_filter = False)
                dfjoin = pd.read_excel(fname, sheet_name='Joining condition',na_filter = False)
                dfjoin.columns = dfjoin.columns.str.strip().str.upper() 
                df.columns = df.columns.str.strip().str.upper()
#                 print df.columns
                dfx=df.filter(items=['TRACEBILITY ID','SEQ NO.','TARGET PHYSICAL TABLE NAME','TARGET PHYSICAL COLUMN NAME','PRIMARY KEY INDICATOR','SOURCE SYSTEM','SOURCE TABLE(S)','SOURCE FIELD(S)/ COLUMN(S)','OFFSET COLUMN','DEPENDENT ON TARGET TABLE','DEPENDENT ON TABLE','DEPENDENT ON COLUMN','PROCESS CATEGORY','TRANSFORMATION RULE','DEFAULT VALUE','JOIN REFERENCE','STATUS','CREATE DATE','UPDATE DATE'])
#               dfx = dfx.assign(Sequence_ID=[i for i in (xrange(len(dfx)))])[['Sequence_ID'] + dfx.columns.tolist()]
                dfjc=dfjoin[(dfjoin.STATUS=='Active') & (pd.notnull(dfjoin['CREATE DATE']))]
                #print dfjc
                newdf=dfx[pd.notnull(dfx['CREATE DATE'])]
                newdf['OFFSET COLUMN'] = map(lambda x: x.strip().upper(), newdf['OFFSET COLUMN'])
                #print len(newdf ['Tracebility ID'])
                #print len(newdf ['Tracebility ID'])    
                retval=readXlsValidator.validate_workbook_dtls(newdf)
                #readXlsAuditor.log_audit('reader_orchestrator()',newdf)

                #print retval[1]
                returndf=retval[1]
                #print returndf['Status']
                validdf=returndf[(returndf['VALIDATION STATUS']=='Y') & (returndf['STATUS']=='Active')]
                invaliddf=returndf[returndf['VALIDATION STATUS']=='N']
                offset_df=newdf[newdf['OFFSET COLUMN']=='Y']
                offset_df=offset_df.filter(items=['TARGET PHYSICAL TABLE NAME','TARGET PHYSICAL COLUMN NAME','SOURCE FIELD(S)/ COLUMN(S)'])
                offset_df.columns=['TARGET PHYSICAL TABLE NAME','TARGETOFFSETCOLUMN','SOURCEOFFSETCOLUMN']
                #print len(offset_df['Target Physical Table Name'])
                if not offset_df.empty:
                    validdf=pd.merge(validdf,offset_df,how='left', on='TARGET PHYSICAL TABLE NAME')
                #print invaliddf['Tracebility ID']
#               print 'retval[0]' + retval[0]
                if retval[0] == 'True':
                    #print validdf
#                   for index,row in validdf.iterrows():
#                       print str(row['Sequence_ID']) + '=' + row['Tracebility ID']

#                   for index,row in validdf.iterrows():
#                       print str(row['Sequence_ID']) + '=' + row['Tracebility ID']
                         
                    final_df = baseParser.parse_data(validdf,dfjc)
#                   print final_df
                    #print compared_default
                    #xlswriter.write_xls(compared_default, 'username')
                    #xlswriter.write_xls(rdmjointype, 'username')
                    classified_df=classifier.classify_testCases(final_df)
                    xlswriter.write_xls_classification(classified_df)
                    final_df['Test Case No']=(final_df['Mapping Reference Number']+final_df['Test case Category']+final_df['ProcessCategory']).rank(method='dense').astype(int)
#                     xlswriter.write_xls(final_df)
                    final_fname=xlswriter.write_xls(final_df)
#                     print test_case_fname
#                     return test_case_fname
#                     return finalwb
                    self.log.info('*****Processing Complete..........')
#                     print final_fname
                    return final_fname  
#                     else:
#                         exceptionmsg=readXlsError.check_Business_Exception("Mapping_Doc_Error") 
#                         self.log.error(exceptionmsg)    
#                         exit()
        #             for index,row in final_df.iterrows():
        #                     print row['Tracebility ID'] + '=' + row['TestCase'] + '=' + row['TargetXLSTab'] + '=' + row['ProcessCategory']
            except Exception as e:
                exceptionmsg=self.errhandler.check_Technical_Exception(e)
                print exceptionmsg
                self.log.error(exceptionmsg)
        else:
            #print 'ok'
            exceptionmsg=self.errhandler.check_Business_Exception("Tab_Not_Found")
            print exceptionmsg  
            self.log.error(exceptionmsg)    
            exit()

# try:  
# #     #df1 = pd.read_excel('EDWard_MBR_SIMPLY_PBM_Mapping_Standard_V0.1(1).xlsx', sheet_name='Attribute Mapping',na_filter = False)
#     test = testCaseGenerationOrchestartor()
# #     #test.readXls('EDWard_MBR_SIMPLY_PBM_Mapping_Standard_Sample.xlsx')
# #     #test.readXls('EDWard_MBR_SIMPLY_PBM_Mapping_Standard_V0.1(1).xlsx')
# #     
# #     #test.reader_orchestrator('EDWard_MBR_SIMPLY_PBM_Mapping_Standard_V0.1_KAUSTAV.xlsx')
#     test.reader_orchestrator(os.path.join(testCaseGenerationConfig.maping_path,testCaseGenerationConfig.mapping_filename))
# #     
# except Exception as e:
#     print e  
             