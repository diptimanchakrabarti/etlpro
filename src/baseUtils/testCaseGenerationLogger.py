'''
Created on Nov 9, 2017

@author: IBM
'''
import logging
import logging.handlers
import os
import sys

import datetime
import time
from mainController import testCaseGenerationConfig

class testCaseGenerationLogger(object):

 def __init__(self):
    pass

 def getLogger(self,name='root', loglevel='DEBUG'):
      logger = logging.getLogger(name)

      # if logger 'name' already exists, return it to avoid logging duplicate
      # messages by attaching multiple handlers of the same type
      if logger.handlers:
        return logger
      # if logger 'name' does not already exist, create it and attach handlers
      else:
        # set logLevel to loglevel or to INFO if requested level is incorrect
        loglevel = getattr(logging, loglevel.upper(), logging.DEBUG)
        logger.setLevel(loglevel)
        #fmt = '%(asctime)s %(filename)-18s %(levelname)-8s: %(message)s'
        fmt = "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s- %(message)s"
#         fmt_date = '%Y-%m-%d-%H-%M-%S'
        fmt_date = '%Y-%m-%d-%H-%M-%S'
        formatter = logging.Formatter(fmt, fmt_date)
        handler1 = logging.StreamHandler()

        ts = time.time()
        #st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H-%M-%S')
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
        #print type(st)
        #print st
        filename = testCaseGenerationConfig.log_file_path + st + '_' + testCaseGenerationConfig.log_file_mask + '.log' 
        #+ 'example_' + st + '.log'
        #print filename
        #handler2 = logging.handlers.RotatingFileHandler(filename,mode='a',maxBytes=5000,backupCount=20)
        #handler2 = logging.FileHandler(filename, mode='a', encoding=None, delay=False)
        handler2 = logging.FileHandler(filename, encoding=None, delay=False)
        handler1.setFormatter(formatter)
        handler2.setFormatter(formatter)
        handler_log_level = testCaseGenerationConfig.log_level
        
        
        if(handler_log_level.strip().upper() == 'ERROR'):
           handler1.setLevel(logging.ERROR) 
           handler2.setLevel(logging.ERROR)    
        if(handler_log_level.strip().upper() == 'WARNING'):
           handler1.setLevel(logging.WARNING) 
           handler2.setLevel(logging.WARNING)    
        if(handler_log_level.strip().upper() == 'INFO'):
           handler1.setLevel(logging.INFO) 
           handler2.setLevel(logging.INFO)    
        if(handler_log_level.strip().upper() == 'DEBUG'):
           handler1.setLevel(logging.DEBUG) 
           handler2.setLevel(logging.DEBUG)    
                
        
        logger.addHandler(handler1)
        logger.addHandler(handler2)
        

        if logger.name == 'root':
          logger.warning('Running: %s %s',
                         os.path.basename(sys.argv[0]),
                         ' '.join(sys.argv[1:]))
        return logger
 