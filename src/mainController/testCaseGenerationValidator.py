'''
Created on Nov 9, 2017

@author: IBM
'''
from string import punctuation as punc
from baseUtils import testCaseGenerationLogger
import re
from baseUtils import testCaseGenerationAuditor
#from baseUtils import testCaseGenerationErrorHandler
import pandas as pd
#import string
#from pandas import ExcelFile
from mainController import testCaseGenerationConfig

class testCaseGenerationValidator():
    def __init__(self):
        logger=testCaseGenerationLogger.testCaseGenerationLogger()
        self.log = logger.getLogger(self.__class__.__name__)
        #self.log.info('This is logger for testCaseGenerationValidator:init method')
        

    def threshold_validator(self,valdf):
        threshold=0.5
        if round(len(filter(lambda x: x in 'N',valdf['VALIDATION STATUS'])),2)/round(len(valdf),2) > threshold:
            return ('False',valdf)
        else:
            return ('True',valdf)

    def validate_workbook(self,wbname):
        mandatory_sheet=['Attribute Mapping','Joining Condition']
        #wb=xlrd.open_workbook(wbname)
        wb=pd.ExcelFile(wbname)
        sheetnames=wb.sheet_names
#         print str(sheetnames).upper()
        for sheet in mandatory_sheet:
            if sheet.upper() in str(sheetnames).upper():
                valid=0
            else:
                valid=1
        for wrksht in sheetnames:
            if str(wrksht).strip().upper() == 'ATTRIBUTE MAPPING':
                if valid==0 and len(pd.read_excel(wbname, sheetnames.index(wrksht))) > 1:
#                     print "Success"
                    return True
                else:
#                     print "Fail"
                    return False

    def validate_case_step(self,valdf):
        #Check for CASE WHEN...THEN...ELSE...END
        for index,row in valdf.iterrows():
            if str(row['PROCESS CATEGORY']).strip().upper()=='TRANSFORM':
                if re.search(r"(^CASE WHEN)(.*)(?=THEN)(.*)(?=ELSE)(.*)(?=END)",row["TRANSFORMATION RULE"],re.IGNORECASE)<>None or re.search(r"(^step[0-9]:)(.*)",row["TRANSFORMATION RULE"],re.IGNORECASE)<>None:
                    #Validation Success
                    #valdf.ix[index,valdf.columns.get_loc('Validation Status')]='Y'
                    pass
                else:
                    #Validation Failure
                    valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
                    #print xlsdf['Validation Status']
        self.log.info("Transform rule format validation complete.")
                
    def validate_status(self,valdf):
        #CHECK for status
        status=['ACTIVE','INACTIVE']
        for index,row in valdf.iterrows():
            if str(row['STATUS']).strip().upper() in status:
                #valdf.ix[index,valdf.columns.get_loc('Validation Status')]='Y'
                pass
            else:
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
        self.log.info("Status Validation complete")
    
    def validate_special_char(self,valdf):
        #keywords=('SUBSTR','INSTR')
        for index,row in valdf.iterrows():
            punc_match=0
            if str(row['SOURCE FIELD(S)/ COLUMN(S)']).strip()!='' and  re.search(r"^N(\/?)A$",str(row['SOURCE FIELD(S)/ COLUMN(S)']).strip(),re.IGNORECASE)==None:           
                for char_col in str(row['SOURCE FIELD(S)/ COLUMN(S)']).strip():
                    if (char_col != '_' )and char_col in list(punc):
                        punc_match=1
                if punc_match > 0:
                    valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
#             for keys in keywords:
#                 if re.match(keys+r"\((.*)\)",str(row['Source Field(s)/ Column(s)']).strip().upper()) <> None and (str(re.findall(keys+r"\((.*)\)",str(row['Source Field(s)/ Column(s)']).strip().upper())[0]).split(',')[0]).strip()[-1] == '_':
#                     valdf.ix[index,valdf.columns.get_loc('Validation Status')]+=1               
#             if row['Source Field(s)/ Column(s)'] and str(row['Source Field(s)/ Column(s)']).strip()[-1] == '_':
#                 valdf.ix[index,valdf.columns.get_loc('Validation Status')]+=1
        self.log.info("Source File format Validation complete.") 
        
    def validate_target_blank(self,valdf):
        for index,row in valdf.iterrows():
            #if pd.notnull(row['Target Physical Table Name ']) and pd.notnull(row['Target Physical Column Name']):
            if row['TARGET PHYSICAL TABLE NAME'] and row['TARGET PHYSICAL COLUMN NAME']:
                #valdf.ix[index,valdf.columns.get_loc('Validation Status')]+=1
                pass
            else:
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
        self.log.info("Target table/column not null Validation complete.")
        
    def validate_process_category(self,valdf):
        proc_cat=['LOAD AUDIT', 'DEFAULT', 'TRANSFORM', 'STRAIGHT', 'KEY', 'DERIVED']
        for index,row in valdf.iterrows():
            if str(row['PROCESS CATEGORY']).strip().upper() in proc_cat:
                #valdf.ix[index,valdf.columns.get_loc('Validation Status')]='Y'
                pass
            else:
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
        self.log.info("Process category Validation complete.")
        
    def validate_not_null_default(self,valdf):
        for index,row in valdf.iterrows():
            if str(row['PROCESS CATEGORY']).strip().upper()=='DEFAULT' and (not row['DEFAULT VALUE']):
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
            elif str(row['PROCESS CATEGORY']).strip().upper()=='TRANSFORM' and (re.search('(.*)DEFAULT(.*)',row['DEFAULT VALUE'],re.IGNORECASE)<>None) and (not row['DEFAULT VALUE']):
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
            else:
                pass
        self.log.info("Not Null default Validation validation.")
        
    def validate_source_dtls(self,valdf):
        for index,row in valdf.iterrows():
            if str(row['PROCESS CATEGORY']).strip().upper() in ['STRAIGHT','TRANSFORM'] and not (row['SOURCE TABLE(S)'] and ['SOURCE FIELD(S)/ COLUMN(S)']):
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
            else:
                #valdf.ix[index,valdf.columns.get_loc('Validation Status')]+=1
                pass
        self.log.info("Source table/column not null Validation complete.")
        
#     def validate_default_values(self,valdf):
#         default_list=[]#No sample data yet
#         for index,row in valdf.iterrows():
#             if str(row['Process Category']).strip.upper()=='Default' and str(row['Default Value']).strip().upper() in default_list:
#                 valdf.ix[index,valdf.columns.get_loc('Validation Status')]='Y'
#             else:
#                 valdf.ix[index,valdf.columns.get_loc('Validation Status')]='N'
            #print "Default values Validation done.."
                
    def validate_column_format(self,valdf):
        cond_list=[]
        for index,row in valdf.iterrows():
            if str(row['PROCESS CATEGORY']).strip().upper()=='TRANSFORM':
                if re.search(r"(^CASE WHEN)(.*)(?=THEN)(.*)(?=ELSE)(.*)(?=END)",str(row['TRANSFORMATION RULE']).strip(),re.IGNORECASE)<>None:
                    cond_list=re.findall(r"(?<=WHEN)(.*?)(?=THEN)",str(row['TRANSFORMATION RULE']).strip(), re.IGNORECASE)
#                     if re.search(r"(?<=THEN)(.*?)(?=WHEN)",str(row['Transformation Rule']).strip(), re.IGNORECASE)<>None:
#                         cond_list.append(re.findall(r"(?<=THEN)(.*?)(?=WHEN)",str(row['Transformation Rule']).strip(), re.IGNORECASE))
                if re.search(r"(^step[0-9]:)(.*)",str(row['TRANSFORMATION RULE']).strip().replace("\n"," "),re.IGNORECASE)<>None:
                    cond_list=re.findall(r"step[0-9]+[:](.+)",str(row['TRANSFORMATION RULE']).strip(),re.IGNORECASE)
        #vald=0
        #print cond_list
        for cond_index in range(len(cond_list)):
            if re.search(r"\w+[.]\w+",cond_list[cond_index],re.IGNORECASE)<>None:
                pass
            else:
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
                #print 'error in validation'
        self.log.info("column name format Validation complete.")
#         return valdf

    def validate_duplicate(self,valdf):
        dup_recs = valdf[valdf.duplicated('TRACEBILITY ID', keep=False)]['TRACEBILITY ID'].unique()
        for index,row in valdf.iterrows():
            if row['TRACEBILITY ID'] in dup_recs:
                valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1

        self.log.info("Duplicate mapping Validation complete.")
        
    def validate_offset(self,valdf):
        tgt_offset_list=[]
        if str(testCaseGenerationConfig.sourceSystemName).strip().upper() == 'SQLSERVER':
            for tgt_tab_name in valdf['TARGET PHYSICAL TABLE NAME'].unique():
                tgt_off_match=0
                for index,row_tgt in valdf.iterrows():
                    if ((tgt_tab_name.strip().upper() not in ['NA','N/A']) and (str(row_tgt['TARGET PHYSICAL TABLE NAME']).strip().upper()==tgt_tab_name.strip().upper()) and (row_tgt['OFFSET COLUMN']=='Y')):
                        tgt_off_match+=1
                if (tgt_off_match==0 and (tgt_tab_name.strip().upper() not in ['NA','N/A'])):
                    tgt_offset_list.append(str(tgt_tab_name).strip().upper())
                  
            for index,row in valdf.iterrows():
                if str(row['TARGET PHYSICAL TABLE NAME']).strip().upper() in tgt_offset_list:
                    valdf.ix[index,valdf.columns.get_loc('VALIDATION STATUS')]+=1
        
        self.log.info("Offset Validation complete.")
                                           
    def validate_workbook_dtls(self,xlsdf):
        xlsdf["VALIDATION STATUS"]=0
        self.log.info('*****START=Mapping file field level validation.')
        self.validate_status(xlsdf)
        self.validate_special_char(xlsdf)
        self.validate_target_blank(xlsdf)
        self.validate_process_category(xlsdf)
        self.validate_case_step(xlsdf)
        self.validate_not_null_default(xlsdf)
        self.validate_source_dtls(xlsdf)
        #self.validate_default_values(xlsdf)
        self.validate_duplicate(xlsdf)
        self.validate_column_format(xlsdf)
        self.validate_offset(xlsdf)
#         print xlsdf
        self.log.info('*****END=Mapping file field level validation.')
#         print xlsdf
        #print xlsdf["Validation Status"]
        xlsdf["VALIDATION STATUS"]=map(lambda x: "N" if x >0 else "Y",xlsdf["VALIDATION STATUS"])
#         for index,row in xlsdf.iterrows():#Write a lambda function
#             if row["Validation Status"] == 0:
#                 xlsdf.ix[index,xlsdf.columns.get_loc("Validation Status")]='Y'
#             else:
#                 xlsdf.ix[index,xlsdf.columns.get_loc("Validation Status")]='N'
        #print xlsdf["Validation Status"]
        xlsAuditor=testCaseGenerationAuditor.testCaseGenerationAuditor()
        xlsAuditor.log_audit('validation',xlsdf)
        return self.threshold_validator(xlsdf)   
            
    