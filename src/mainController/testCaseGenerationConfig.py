'''
Created on Dec 11, 2017

@author: IBM
'''

import yaml
import sys
import os
import commands


osType=sys.platform

configFileName='TCGA.yml'
if 'WIN' in osType.upper():
	pyPath=os.path.abspath(os.path.curdir)
	targetPath=pyPath+'\\config\\'+configFileName
else:
	pyPath=commands.getoutput('echo $PYTHONPATH')
	shellPath=commands.getoutput('pwd')
	tctPath=os.path.abspath('TCAT.yml')
	targetPath=pyPath+'/config/'+configFileName

    
   
# def get_config_data():
     
with open(targetPath) as f:
    dataMap = yaml.safe_load(f)
           
user_name=dataMap['user']
sourceSystemName=dataMap['sourceSystem']['sourceSystemName']
destinationSystemName=dataMap['targetSystem']['targetSystemName']
maping_path=pyPath+dataMap['source']['path']
mapping_filename=dataMap['source']['fileName']
testcase_destination_path=pyPath+dataMap['destination']['destinationPath']
testcase_destination_filemask=dataMap['destination']['destinationFile']
query_template_path=pyPath+dataMap['Querytemplate']['filepath']
query_template_fname=dataMap['Querytemplate']['filename']
errorlistdict=dataMap['error']
audit_level_dict=dataMap['audit']
log_file_path=pyPath+dataMap['log']['filepath']
log_file_mask=dataMap['log']['filemask']
log_level=dataMap['log']['loglevel']
# log_date_format=dataMap['log']['logdateformat']
download_filename=''
          
def update_config(src_sys,tgt_sys):
    with open(targetPath) as f:
        dataMap = yaml.safe_load(f)
      
    dataMap['sourceSystem']['sourceSystemName']=src_sys
    dataMap['targetSystem']['targetSystemName']=tgt_sys
    
      
    with open(targetPath,'w') as yaml_file:
        yaml_file.write(yaml.dump(dataMap, default_flow_style=False))
