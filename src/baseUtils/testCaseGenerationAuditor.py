'''
Created on Nov 9, 2017

@author: IBM
'''
import os
from os import system
import sys
import pandas as pd
from baseUtils import testCaseGenerationErrorHandler
from baseUtils import testCaseGenerationLogger
from mainController import testCaseGenerationConfig

class testCaseGenerationAuditor(object):
  
    def __init__(self):
        logger=testCaseGenerationLogger.testCaseGenerationLogger()
        self.log = logger.getLogger(self.__class__.__name__)
        #self.log.info('This is logger for testCaseGenerationAuditor:init method')
    
              
    def log_audit(self,operationType,querydf):
        #auditlevel={'reader_orchestrator()':'Process Category','classify_testCases()':'Query Type','validate_workbook_dtls()':'Validation Status'}
        auditlevel=testCaseGenerationConfig.audit_level_dict 
        #self.log.info('This is logger for getAuditor')
        if auditlevel.get(operationType) <> None:
            querydf[auditlevel[operationType].strip().upper()]=map(lambda x: x.strip().upper(),querydf[auditlevel[operationType].strip().upper()])
            for uniqtype in querydf[auditlevel[operationType].strip().upper()].unique():
                cnt=len(filter(lambda x: uniqtype in x,querydf[auditlevel[operationType].strip().upper()]))
                msg = str(operationType) + " - **AUDIT INFO** - " + str(cnt) + ' Records processed for ' + str(auditlevel[operationType].strip().upper()) + ' "' + str(uniqtype) +'"' 
                self.log.info(msg)
        else:
            msg = str(operationType) + " - **AUDIT INFO** - "  + str(len(querydf)) + ' Records processed.'
            self.log.info(msg) 
        
