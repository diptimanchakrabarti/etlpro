'''
Created on Nov 9, 2017

@author: IBM
'''

import sys
from baseUtils import testCaseGenerationLogger
from mainController import testCaseGenerationConfig
#from mainController import testCaseGenerationConfig
import logging



import yaml


# with open('C:\\Users\\IBM_ADMIN\\workspacepython\\testCaseGeneratorUtilProject\\src\\baseUtils\\TCGA.yml') as f:
#     dataMap = yaml.safe_load(f)

#print dataMap

# for k,v in dataMap.iteritems(): 
#     if k=='error':
#          errlistdict=v
#          #print errlistdict
    

class testCaseGenerationErrorHandler(object):

  def __init__(self):
    xls_logger = testCaseGenerationLogger.testCaseGenerationLogger()
    self.log = xls_logger.getLogger(self.__class__.__name__)
    self.errlistdict=testCaseGenerationConfig.errorlistdict
    #print self.errlistdict
#     testCaseGenerationConfig.get_config_data()
#     self.app_config=testCaseGenerationConfig.testCaseGenerationConfig()
#     self.errlistdict=self.app_config.get_config_data('error')

    #self.log.info('Log from ErrorHandler')

    
  def check_Technical_Exception(self,object1):
    print 'statusofkey' +  str(self.errlistdict.has_key(type(object1).__name__))
    
    if ( (self.errlistdict.has_key(type(object1).__name__)) == True):
        return self.errlistdict[type(object1).__name__]
    else:
       return  self.errlistdict['Generic_Error']

#   if errlistdict[type(object1).__name__] <> None:
#           return errlistdict[type(object1).__name__]
#   else:
#          return  errlistdict['Generic_Error']
    
  def check_Business_Exception(self,strerr):
      if ( (self.errlistdict.has_key(strerr)) == True):
        return self.errlistdict[strerr]
      else:
       return  self.errlistdict['Generic_Error']
  
#       
#       
#       if errlistdict[strerr] <> None:
#           return errlistdict[strerr]
#       else:
#          return  errlistdict['Generic_Error']
           
  

