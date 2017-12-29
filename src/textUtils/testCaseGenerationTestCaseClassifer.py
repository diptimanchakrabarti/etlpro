'''
Created on Nov 9, 2017

@author: IBM
'''
from baseUtils import testCaseGenerationLogger
from baseUtils import testCaseGenerationAuditor
import pandas as pd

class testCaseGenerationTestCaseClassifier(object):

    def __init__(self):
        logger =   testCaseGenerationLogger.testCaseGenerationLogger()
        self.log = logger.getLogger(self.__class__.__name__)

    def classify_testCases(self,finaldf):
        classifierAuditor=testCaseGenerationAuditor.testCaseGenerationAuditor()
#         classifierAuditor.log_audit('classify', finaldf)
        self.log.info("*****START=Classify generated test cases.")
        classify_auditdf=pd.DataFrame([])
        classify_df=pd.DataFrame([])
        for proc_cat in finaldf['ProcessCategory'].unique():
            temp_df=finaldf[finaldf['ProcessCategory']==proc_cat]
            for trace_id in temp_df['Mapping Reference Number'].unique():
                classify_auditdf=classify_auditdf.append(pd.DataFrame([[trace_id,proc_cat]],columns=['MAPPING REFERENCE NUMBER','PROCESSCATEGORY']))
            classify_df=classify_df.append(pd.DataFrame([[proc_cat,len(temp_df['Mapping Reference Number'].unique()),len(temp_df)]],columns=['ProcessCategory','CountOfMapping','TestCaseCount']))
        
#         print classify_df
        classifierAuditor.log_audit('classify', classify_auditdf)
        self.log.info("*****END=Classify generated test cases.")
        return classify_df
        
            
            