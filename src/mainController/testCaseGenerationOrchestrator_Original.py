'''
Created on Nov 9, 2017

@author: IBM
'''

import os
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from baseUtils import testCaseGenerationLogger
from baseUtils import testCaseGenerationAuditor
from baseUtils import testCaseGenerationErrorHandler
from readerUtils import testCaseGenerationValidator
from writerUtil import testCaseGenerationXLSWriter

from textUtils import testCaseGenerationBaseParser


class testCaseGenerationOrchestartor(object):
    def __init__(self):
        logger =   testCaseGenerationLogger.testCaseGenerationLogger()
        self.log = logger.getLogger(self.__class__.__name__)
        self.log.info('This is logger for testCaseGenerationOrchestrator:init method')
    
    def readXls(self,fname):
        readXlsAuditor=testCaseGenerationAuditor.testCaseGenerationAuditor()
        readXlsError=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
        readXlsValidator = testCaseGenerationValidator.testCaseGenerationValidator()
        baseParser = testCaseGenerationBaseParser.testCaseGenerationBaseParser()
        xlswriter = testCaseGenerationXLSWriter.testCaseGenerationXLSWriter()
        try:
            
           

            df = pd.read_excel(fname, sheet_name='Attribute Mapping', skiprows=1,na_filter = False)
            dfx=df.filter(items=['Tracebility ID','Seq No.','Target Physical Table Name','Target Physical Column Name','Primary Key Indicator','Source System','Source table(s)','Source Field(s)/ Column(s)','Dependent on Target Table','Dependent on Table','Dependent on Column','Process Category','Transformation Rule','Default Value','Join Reference','Status','Create Date','Update Date'])
            maindf=dfx[(dfx.Status=='Active') & (pd.notnull(dfx['Create Date']))]
            #print maindf
            #final_default = baseParser.baseparser_for_defaultType(maindf)
            
            dfjoin = pd.read_excel(fname, sheet_name='Joining condition',na_filter = False)
            dfjc=dfjoin[(dfjoin.Status=='Active') & (pd.notnull(dfjoin['Create Date']))] 
            
            #final_default = baseParser.parse_data(maindf,dfjc)
            #print maindf
            #final_default = baseParser.parse_data(maindf,dfjc)
            final_df = baseParser.parse_data(maindf,dfjc)
            
#             for index,row in final_df.iterrows():
#                     print row['Tracebility ID'] + '=' + row['TestCase'] + '=' + row['TargetXLSTab'] + '=' + row['ProcessCategory'] 
             

  
            xlswriter.write_xls(final_df, 'Anthem')

            #print (newdf['Process Category'])
            #readXlsAuditor.getAuditor('readXls()',newdf)
            #readXlsValidator = testCaseGenerationValidator.testCaseGenerationValidator()
            #retval=readXlsValidator.xlsValidator(newdf)
            #print retval
#             returndf=retval[1]
#             if retval[0] == 'True':
#                 for 'Y' in returndf['Validation Status']:
#                     validdf.append()
            
        except Exception as e:
            #print e
            #exceptionmsg=readXlsError.checkTechnicalException(e)
            #print exceptionmsg
            print e
            

#print 'Call xls read'
# orchestrator = testCaseGenerationOrchestartor()
# orchestrator.readXls('C:\\Users\\IBM_ADMIN\\workspacepython\\testCaseGeneratorUtilProject\\src\\mainController\\EDWard_MBR_SIMPLY_PBM_Mapping_Standard_V0.1(1).xlsx')

try:  
    #df1 = pd.read_excel('EDWard_MBR_SIMPLY_PBM_Mapping_Standard_V0.1(1).xlsx', sheet_name='Attribute Mapping',na_filter = False)
    test = testCaseGenerationOrchestartor()
    #test.readXls('EDWard_MBR_SIMPLY_PBM_Mapping_Standard_Sample.xlsx')
    test.readXls('EDWard_MBR_SIMPLY_PBM_Mapping_Standard_V0.1(1).xlsx')
    
except Exception as e:
    print e  
            