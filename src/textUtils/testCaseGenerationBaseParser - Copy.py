'''
Created on Nov 9, 2017

@author: IBM
'''
import pandas as pd
import re
import datetime
#import string
import os
#from click._compat import iteritems
from mainController import testCaseGenerationConfig
from textUtils import testCaseGenerationTestSQLGenerator
from baseUtils import testCaseGenerationErrorHandler
from baseUtils import testCaseGenerationLogger

class testCaseGenerationBaseParser(object):
    
    parseError=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
    
    def __init__(self):
        logger =   testCaseGenerationLogger.testCaseGenerationLogger()
        self.log = logger.getLogger(self.__class__.__name__) 
        self.errhandler=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
        self.query_template_fname=os.path.join(testCaseGenerationConfig.query_template_path,testCaseGenerationConfig.query_template_fname)
        #print self.query_template_fname
        
    
    def parse_data(self,mapdf,joindf):
        try:
            #RDM join Type Logic and invocation
            self.log.info('*****START=Parsing text')
            joindf=joindf[joindf['Status']=='Active']
            joindf=joindf.filter(items=['Reference Number','Source Table Linkage/ LookUp','Joining Condition'])
            if not joindf.empty:
                rdmmapdf=mapdf.merge(joindf, left_on='Join Reference', right_on='Reference Number')
                rdmmapdf['Process Category'] = map(lambda x: x.strip().upper(), rdmmapdf['Process Category'])
                rdmmapdf=rdmmapdf[(rdmmapdf['Process Category']=='TRANSFORM') & (pd.notnull(rdmmapdf['Join Reference'])) & (rdmmapdf['Source Table Linkage/ LookUp'].str.contains('RDM'))]
            else:
                rdmmapdf=mapdf[(mapdf['Process Category']=='TRANSFORM') & (pd.notnull(mapdf['Join Reference']))]
            #print rdmmapdf
            
            final_rdm=self.baseparser_for_rdmjoinType(rdmmapdf)
#             print 'final_rdm'
#             print final_rdm
              
            final_default = self.baseparser_for_defaultType(mapdf)
#             print 'final_default'
#             print final_default
            final_straight = self.baseparser_for_straightType(mapdf)
#             print 'final_straight'
#             print final_straight
            final_gen=self.baseparser_for_generic(mapdf)
            
            final_transform=self.baseparser_for_TransformType(mapdf)
            
            default_straight_df = final_default.append(final_straight,ignore_index=True)
            final_df = default_straight_df.append(final_rdm,ignore_index=True)
            final_df = final_df.append(final_transform,ignore_index=True)
            final_df=final_df.sort_values(by=['ProcessCategory','Tracebility ID','Test case Category','Query Type'])
            final_df = final_df.append(final_gen,ignore_index=True)
            self.log.info('*****END=Parsing text')

            return final_df
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg  
    
        
      
    def baseparser_for_straightType(self,maindf):
        try:  
              self.log.info('*****START=Parse text for Straight Type')
              
              dfQueryTemplate_straight = pd.read_excel(self.query_template_fname, sheet_name='Query Template', na_filter = False)
              ########## Join and get template###########
              templatedf_straight = dfQueryTemplate_straight[(dfQueryTemplate_straight['Transform Category']=='Straight')]
              df_straight_cat_map = pd.read_excel(self.query_template_fname, sheet_name='Category Mapping', na_filter = False)
              df_straight_cat_map=df_straight_cat_map.filter(items=['Transform Category','Test category'])
              templatedf_straight=pd.merge(templatedf_straight,df_straight_cat_map,how='inner',left_on=['Transform Category','Test case Category'],right_on=['Transform Category','Test category'])
              templatedf_straight=templatedf_straight.drop(columns=['Test category'])
              
              ################ get template for base############
              #test_area_type_source='Hive'#Should come from config
              test_area_type_source=testCaseGenerationConfig.sourceSystemName
              templatedf_base_straight=templatedf_straight[(templatedf_straight['Query Type']=='Base') & (templatedf_straight['Test Area Type']==test_area_type_source)]
              
              ############Get template for compare#########
              #test_area_type_target='Hive'#Should come from config
              test_area_type_target=testCaseGenerationConfig.destinationSystemName
              templatedf_compare_straight = templatedf_straight[(templatedf_straight['Query Type']=='Compare') & (templatedf_straight['Test Area Type']==test_area_type_target)]
              
              #########Create var-con for baase and compare########
              dfvarOnMap_straight = pd.read_excel(self.query_template_fname, sheet_name='var-con Map', na_filter = False)
              dfvarOnMap_straight = dfvarOnMap_straight[dfvarOnMap_straight['Category']=='Straight']
              vardf_basestraight=pd.DataFrame(data=None,columns=dfvarOnMap_straight.columns)
              
              for seq in templatedf_base_straight['SequenceNo'].unique():
                  vardf_basestraight=vardf_basestraight.append(dfvarOnMap_straight[dfvarOnMap_straight['QuerySequenceNo']==seq])
              #print vardf_basestraight
              
              vardf_comparestraight=pd.DataFrame(data=None,columns=dfvarOnMap_straight.columns)
              
              for seq in templatedf_compare_straight['SequenceNo'].unique():
                  vardf_comparestraight=vardf_comparestraight.append(dfvarOnMap_straight[dfvarOnMap_straight['QuerySequenceNo']==seq])
              
              maindf['Process Category'] = map(lambda x: x.strip().upper(), maindf['Process Category'])
              maindf = maindf[maindf['Process Category']=='STRAIGHT']

              final_vardf_base_straight=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
              final_vardf_comapre_straight=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
              count = 0
              for index,row in maindf.iterrows():
                  
                    #print row['Tracebility ID']   
                    for index,row_bdf in vardf_basestraight.iterrows():
#                         print row_bdf['AttributeName']
                        if (str(row_bdf['AttributeName']).strip()=='sourceColumnName'):
                               #row_bdf['AttributeName'] = row['Source Field(s)/ Column(s)']
                               final_vardf_base_straight = final_vardf_base_straight.append(pd.DataFrame([[row['Tracebility ID'],row_bdf['QuerySequenceNo'],row_bdf['Category'],row_bdf['Query Type'],row_bdf['AttributeType'],row_bdf['ColumnHeader'],row_bdf['AttributeName'],row[row_bdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
        #                 if (str(row_bdf['AttributeName']).strip()=='sourceColumnName'):  
        #                        row_bdf['AttributeName'] = row['Source Field(s)/ Column(s)'] 
        #                        final_vardf_base_straight = final_vardf_base_straight.append(pd.DataFrame([[row['Tracebility ID'],row_bdf['QuerySequenceNo'],row_bdf['Category'],row_bdf['Query Type'],row_bdf['AttributeType'],row_bdf['ColumnHeader'],row_bdf['AttributeName']]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName']), ignore_index=True)                                        
                        if (str(row_bdf['AttributeName']).strip()=='tableName'):                                                
                               #row_bdf['AttributeName'] = row['Source table(s)']
                               final_vardf_base_straight = final_vardf_base_straight.append(pd.DataFrame([[row['Tracebility ID'],row_bdf['QuerySequenceNo'],row_bdf['Category'],row_bdf['Query Type'],row_bdf['AttributeType'],row_bdf['ColumnHeader'],row_bdf['AttributeName'],row[row_bdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                        if (str(row_bdf['AttributeName']).strip()=='offsetColumn'):
                               final_vardf_base_straight = final_vardf_base_straight.append(pd.DataFrame([[row['Tracebility ID'],row_bdf['QuerySequenceNo'],row_bdf['Category'],row_bdf['Query Type'],row_bdf['AttributeType'],row_bdf['ColumnHeader'],row_bdf['AttributeName'],row[row_bdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                        #print 'comapreDefault: ' + row['Tracebility ID'] + '=' +  row_cdf['AttributeName'] 
                        
                    for index,row_cdf in vardf_comparestraight.iterrows():
                        if (row_cdf['AttributeName']=='columnName'):
                               #row_cdf['AttributeName'] = row['Target Physical Column Name']
                               final_vardf_comapre_straight = final_vardf_comapre_straight.append(pd.DataFrame([[row['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],row[row_cdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                        if (str(row_cdf['AttributeName']).strip()=='Target Physical Column Name'):  
                               #row_cdf['AttributeName'] = row['Target Physical Column Name']
                               final_vardf_comapre_straight = final_vardf_comapre_straight.append(pd.DataFrame([[row['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],row[row_cdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)                                        
                        if (str(row_cdf['AttributeName']).strip()=='tableName'):                                                
                               #row_cdf['AttributeName'] = row['Target Physical Table Name']
                               final_vardf_comapre_straight = final_vardf_comapre_straight.append(pd.DataFrame([[row['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],row[row_cdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                        if (str(row_cdf['AttributeName']).strip()=='offsetColumn'):                                                
                               final_vardf_comapre_straight = final_vardf_comapre_straight.append(pd.DataFrame([[row['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],row[row_cdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)  
                    #print final_vardf_comapre_straight
                    
              straight_dict = {'basetemp_straightdf': pd.DataFrame(templatedf_base_straight),'basevar_straightdf': pd.DataFrame(final_vardf_base_straight),'comparetemp_straightdf': pd.DataFrame(templatedf_compare_straight),'comparevar_straightdf': pd.DataFrame(final_vardf_comapre_straight)}
              #print straight_dict
              self.log.info('*****END=Parse text for Straight Type')
              sqlGenerator1 = testCaseGenerationTestSQLGenerator.testCaseGenerationTestSQLGenerator()
#               #final_straight = sqlGenerator.SQLGeneratorforDefaultType(straight_dict)
              final_straight = sqlGenerator1.SQLGeneratorforStraightType(straight_dict)
              return final_straight
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg   

      
      
      
    def baseparser_for_defaultType(self,maindf):
        try:  
              self.log.info('*****START=Parse text for Default Type')
              
              dfQueryTemplate_default = pd.read_excel(self.query_template_fname, sheet_name='Query Template', na_filter = False)
              ########## Join and get template###########
              templatedf_default = dfQueryTemplate_default[(dfQueryTemplate_default['Transform Category']=='Default')]
              df_default_cat_map = pd.read_excel(self.query_template_fname, sheet_name='Category Mapping', na_filter = False)
              df_default_cat_map=df_default_cat_map.filter(items=['Transform Category','Test category'])
              templatedf_default=pd.merge(templatedf_default,df_default_cat_map,how='inner',left_on=['Transform Category','Test case Category'],right_on=['Transform Category','Test category'])
              templatedf_default=templatedf_default.drop(columns=['Test category'])
              
              ################ get template for base############
              #test_area_type_source='Hive'#Should come from config
              test_area_type_source=testCaseGenerationConfig.sourceSystemName
              templatedf_base_default=templatedf_default[(templatedf_default['Query Type']=='Base') & (templatedf_default['Test Area Type']==test_area_type_source)]
              
              ############Get template for compare#########
              #test_area_type_target='Hive'#Should come from config
              test_area_type_target=testCaseGenerationConfig.destinationSystemName
              templatedf_compare_default = templatedf_default[(templatedf_default['Query Type']=='Compare') & (templatedf_default['Test Area Type']==test_area_type_target)]
              
              #########Create var-con for baase and compare########
              dfvarOnMap_default = pd.read_excel(self.query_template_fname, sheet_name='var-con Map', na_filter = False)
              dfvarOnMap_default = dfvarOnMap_default[dfvarOnMap_default['Category']=='Default']
              vardf_baseDefault=pd.DataFrame(data=None,columns=dfvarOnMap_default.columns)
              
              for seq in templatedf_base_default['SequenceNo'].unique():
                  vardf_baseDefault=vardf_baseDefault.append(dfvarOnMap_default[dfvarOnMap_default['QuerySequenceNo']==seq])
              #print vardf_basestraight
              
              vardf_comparedeafult=pd.DataFrame(data=None,columns=dfvarOnMap_default.columns)
              
              for seq in templatedf_compare_default['SequenceNo'].unique():
                  vardf_comparedeafult=vardf_comparedeafult.append(dfvarOnMap_default[dfvarOnMap_default['QuerySequenceNo']==seq])
              
              maindf['Process Category'] = map(lambda x: x.strip().upper(), maindf['Process Category'])
              maindf = maindf[maindf['Process Category']=='DEFAULT']

              final_vardf_base_default=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
              final_vardf_comapre_default=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
              count = 0
              for index,row in maindf.iterrows():
                  
                    #print row['Tracebility ID']   
                    for index,row_bdf in vardf_baseDefault.iterrows():
#                         print row_bdf['AttributeName']
                        if (str(row_bdf['AttributeName']).strip()=='tableName'):                                                
                               #row_bdf['AttributeName'] = row['Source table(s)']
                               final_vardf_base_default = final_vardf_base_default.append(pd.DataFrame([[row['Tracebility ID'],row_bdf['QuerySequenceNo'],row_bdf['Category'],row_bdf['Query Type'],row_bdf['AttributeType'],row_bdf['ColumnHeader'],row_bdf['AttributeName'],row[row_bdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                        
                    for index,row_cdf in vardf_comparedeafult.iterrows():
                        conditionPart1=''
                        if (row_cdf['AttributeName']=='tableName'):
                               #row_cdf['AttributeName'] = row['Target Physical Column Name']
                               condition_tablename=row[row_cdf['TargetValue'].strip()]
                               final_vardf_comapre_default = final_vardf_comapre_default.append(pd.DataFrame([[row['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],row[row_cdf['TargetValue'].strip()]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                        if (str(row_cdf['AttributeName']).strip()=='condition1'):  
                               conditionVal = condition_tablename \
                                                          + '.' + row[row_cdf['TargetValue'].strip()] + '='  + self.format_function(str(row['Default Value']))
                                                          #+ '\'' + self.format_function(str(row['Default Value'])) + '\'' 
                               #row_cdf['AttributeName'] = row['Target Physical Column Name']
                               final_vardf_comapre_default = final_vardf_comapre_default.append(pd.DataFrame([[row['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],conditionVal]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)                                        
            
              
              default_dict = {'basetemp_defaultdf': pd.DataFrame(templatedf_base_default),'basevar_defaultdf': pd.DataFrame(final_vardf_base_default),'comparetemp_defaultdf': pd.DataFrame(templatedf_compare_default),'comparevar_defaultdf': pd.DataFrame(final_vardf_comapre_default)}
              self.log.info('*****END=Parse text for Default Type')
              sqlGenerator = testCaseGenerationTestSQLGenerator.testCaseGenerationTestSQLGenerator()
              final_default = sqlGenerator.SQLGeneratorforDefaultType(default_dict)
              
              return final_default
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg   

     #########################################################################################################################
      ###RDM JOIN TYPE
      #########################################################################################################################
     
     
     
    def baseparser_for_rdmjoinType(self,maindf):
        try:
            self.log.info('*****START=Parse text for rdmjoinType')
            #maindf['Process Category'] = map(lambda x: x.strip().upper(), maindf['Process Category'])
            #maindf=maindf[(maindf['Process Category']=='TRANSFORM') & (pd.notnull(maindf['Join Reference']))]
            #print maindf
            dfrdmtemplate = pd.read_excel(self.query_template_fname, sheet_name='Query Template',na_filter = False)
            
            ########## Join and get template###########
            templatedf_rdm=dfrdmtemplate[(dfrdmtemplate['Transform Category']=='RDM Join Transform')]
            df_rdm_cat_map = pd.read_excel(self.query_template_fname, sheet_name='Category Mapping', na_filter = False)
            df_rdm_cat_map=df_rdm_cat_map.filter(items=['Transform Category','Test category'])
            templatedf_straight=pd.merge(templatedf_rdm,df_rdm_cat_map,how='inner',left_on=['Transform Category','Test case Category'],right_on=['Transform Category','Test category'])
            templatedf_straight=templatedf_straight.drop(columns=['Test category'])
            
            ################ get template for base############
            #test_area_type_source='Hive'#Should come from config
            test_area_type_source=testCaseGenerationConfig.sourceSystemName
            dfrdm_basetemplate=templatedf_rdm[(templatedf_rdm['Query Type']=='Base') & (templatedf_rdm['Test Area Type']==test_area_type_source)]
            
            #print dfrdm_basetemplate  
            ############Get template for compare#########
            #test_area_type_target='Hive'#Should come from config
            test_area_type_target=testCaseGenerationConfig.destinationSystemName
            dfrdm_cmptemplate = templatedf_rdm[(templatedf_rdm['Query Type']=='Compare') & (templatedf_rdm['Test Area Type']==test_area_type_target)]
            
            #print dfrdm_basetemplate
            #########Create var-con for baase and compare########
            dfvarOnMap_rdm = pd.read_excel(self.query_template_fname, sheet_name='var-con Map', na_filter = False)
            dfvarOnMap_rdm = dfvarOnMap_rdm[dfvarOnMap_rdm['Category']=='RDM Join Transform']
            
            vardf_baserdm=pd.DataFrame(data=None,columns=dfvarOnMap_rdm.columns)
            
            for seq in dfrdm_basetemplate['SequenceNo'].unique():
                vardf_baserdm=vardf_baserdm.append(dfvarOnMap_rdm[dfvarOnMap_rdm['QuerySequenceNo']==seq])            
            
            vardf_comparerdm=pd.DataFrame(data=None,columns=dfvarOnMap_rdm.columns)
            
            for seq in dfrdm_cmptemplate['SequenceNo'].unique():
                vardf_comparerdm=vardf_comparerdm.append(dfvarOnMap_rdm[dfvarOnMap_rdm['QuerySequenceNo']==seq])
            
#             print vardf_comparerdm
#             maindf['Process Category'] = map(lambda x: x.strip().upper(), maindf['Process Category'])
#             maindf = maindf[maindf['Process Category']=='TRANSFORM']
#             rdmbaseiter=int(dfrdmbasetemplate.iloc[0]['Number of Iteration'])
#             rdmcmpiter=int(dfrdmcmptemplate.iloc[0]['Number of Iteration'])
#             dfrdmbasetemplate=dfrdmbasetemplate[dfrdmbasetemplate.columns[0:int(rdmbaseiter+10)]]
#             dfrdmbasetemplate=dfrdmbasetemplate[dfrdmbasetemplate.columns[-int(rdmbaseiter):]]
#             dfrdmcmptemplate=dfrdmcmptemplate[dfrdmcmptemplate.columns[0:int(rdmcmpiter+10)]]
#             dfrdmcmptemplate=dfrdmcmptemplate[dfrdmcmptemplate.columns[-int(rdmcmpiter):]]
    #         print dfrdmbasetemplate
    #         print dfrdmcmptemplate

            dfrdm_basevar=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
            dfrdm_cmpvar=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])

            for index_map,row_map in maindf.iterrows():
                target_tab_nm=row_map['Target Physical Table Name']
                source_tab_nm=row_map['Source table(s)']
                for index_tmpl,row_base in vardf_baserdm.iterrows():
                    #print str(row_base['AttributeName']).strip().upper()
                    if str(row_base['AttributeName']).strip().upper() == 'SOURCECOLUMNNAME':
                        varstring= source_tab_nm + '.' + row_map[row_base['TargetValue']]
                        tempdf=pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],varstring]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
                        dfrdm_basevar=dfrdm_basevar.append(tempdf, ignore_index=True)
                    elif str(row_base['AttributeName']).strip().upper() == 'RDMCOLUMNNAME':
                        colname=str((re.findall(r"\w+[.]\w+",(re.findall(r"step[0-9]+[:](.+)referring",str(row_map[row_base['TargetValue']]).strip(),re.IGNORECASE))[0],re.IGNORECASE))[0])                   
                        dfrdm_basevar=dfrdm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],colname]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
    #                   elif str(row_tmpl['AttributeName']).strip().upper() == 'COLUMNNAME':
    #                   dfrdmbasetmpl=dfrdmbasetmpl.append(pd.DataFrame([[row_map['Tracebility ID'],row_tmpl['Category'],row_tmpl['Query Type'],row_tmpl['ColumnHeader'],row_tmpl['AttributeName'],row_map[row_tmpl['TargetValue']]]],columns=['Tracebility ID','Category','Query Type','ColumnHeader','AttributeName','Value']))
                    elif str(row_base['AttributeName']).strip().upper() == 'TABLENAME':
                        dfrdm_basevar=dfrdm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],row_map[row_base['TargetValue']]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                    elif str(row_base['AttributeName']).strip().upper() == 'RDMTABLE':
                        colnm=str((re.findall(r"\w+[.]\w+",(re.findall(r"step[0-9]+[:](.+)referring",str(row_map[row_base['TargetValue']]).strip(),re.IGNORECASE))[0],re.IGNORECASE))[0])
                        rdbtabnm=colnm.split(".")[0]                      
                        dfrdm_basevar=dfrdm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],rdbtabnm]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                    elif str(row_base['AttributeName']).strip().upper() == 'CONDITION1':
                        #joincond= str(joindf[joindf['Reference Number'] == row_map['Join Reference']].reset_index(drop=True).iloc[0]['Joining Condition'])
                        joincond=str(row_map['Joining Condition']).strip()
                        for txt in joincond.split('AND'):
                            if re.search(str(row_map['Source Field(s)/ Column(s)']).strip(),txt,re.IGNORECASE)<>None:
                                rdmcond1txt=txt
                        dfrdm_basevar=dfrdm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],rdmcond1txt]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True) 
                    elif str(row_base['AttributeName']).strip().upper() == 'CONDITION2':    
                        #joincond= str(joindf[joindf['Reference Number'] == row_map['Join Reference']].reset_index(drop=True).iloc[0]['Joining Condition'])
                        join_where_cond=str(row_map['Joining Condition']).strip()
                        rmcond2txt=''
                        for cond_txt in join_where_cond.split('AND'):
                            if re.search(str(row_map['Source Field(s)/ Column(s)']).strip(),cond_txt,re.IGNORECASE) == None:
                                rmcond2txt=cond_txt.strip() + ' AND ' + rmcond2txt 
                        dfrdm_basevar=dfrdm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],rmcond2txt]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                #print dfrdm_basevar
                    
                for index,row_cdf in vardf_comparerdm.iterrows():
                    #print str(row_cdf['AttributeName']).strip().upper()
                    if str(row_cdf['AttributeName']).strip().upper() == 'COLUMNNAME':
                        targetcolname= target_tab_nm + '.' + row_map[row_cdf['TargetValue']]
                        dfrdm_cmpvar=dfrdm_cmpvar.append(pd.DataFrame([[row_map['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],targetcolname]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                    elif str(row_cdf['AttributeName']).strip().upper() == 'TABLENAME':
                        dfrdm_cmpvar=dfrdm_cmpvar.append(pd.DataFrame([[row_map['Tracebility ID'],row_cdf['QuerySequenceNo'],row_cdf['Category'],row_cdf['Query Type'],row_cdf['AttributeType'],row_cdf['ColumnHeader'],row_cdf['AttributeName'],row_map[row_cdf['TargetValue']]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                #print dfrdm_cmpvar
    
            rdm_dict = {'basetemp_rdmdf': pd.DataFrame(dfrdm_basetemplate),'basevar_rdmdf': pd.DataFrame(dfrdm_basevar),'comparetemp_rdmdf': pd.DataFrame(dfrdm_cmptemplate),'comparevar_rdmdf': pd.DataFrame(dfrdm_cmpvar)}
            #rdm_dict = {'basetemp_rdmdf': pd.DataFrame(dfrdmbasetemplate),'basevar_rdmdf': pd.DataFrame(dfrdmbasetmpl)}
            #print rdm_dict
            self.log.info('*****END=Parse text for rdmjoinType')
            rdmsqlgen=testCaseGenerationTestSQLGenerator.testCaseGenerationTestSQLGenerator()
            rdmgenquerydf=rdmsqlgen.sqlgenerator_for_rdmjoinType(rdm_dict)
            
            return rdmgenquerydf    
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg  
                 
    def format_function(self,valuetoFormat):
        try:
             if re.search(r'^empty( ?)(?=string)',valuetoFormat.strip().lower()):
                return ''
             elif re.search(r'^system( ?)(?=date)',valuetoFormat.strip().lower()):
                 now = datetime.datetime.now()
                 valuetoFormat = ('\'' + now.strftime("%m/%d/%Y")  + '\'')
                 return valuetoFormat
#            elif re.search(r'^~(.*)',valuetoFormat.strip().lower()):
#               return valuetoFormat[1:]
             else:
                if(valuetoFormat.isdigit()):
                  return valuetoFormat 
                else:
                  valuetoFormat=('\'' +  valuetoFormat + '\'')       
                  return  valuetoFormat
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg  
    
    def baseparser_for_generic(self,maindf):
#         src_table_list = maindf['Source table(s)'].unique().upper()
#         tgt_table_List = maindf['Target Physical Table Name'].unique()
        try:
            self.log.info('*****START=Parse text for Generic')
            src_generic_df=pd.DataFrame([])
            for src_tab_name in maindf['Source table(s)'].unique():
                src_off_match=0
                for index,row in maindf.iterrows():
                    if ((src_tab_name.strip().upper() not in ['NA','N/A']) and (str(row['Source table(s)']).strip().upper()==src_tab_name.strip().upper()) and (row['Offset Column']=='Y')):
                        src_generic_df= src_generic_df.append(pd.DataFrame([[row['Source table(s)'],row['SourceOffsetcolumn']]],columns=['Source Table Name','SourceOffsetcolumn']))
                        src_off_match=1
                        break
                if src_off_match==0 and (src_tab_name.strip().upper() not in ['NA','N/A']):
                    src_generic_df= src_generic_df.append(pd.DataFrame([[src_tab_name,'']],columns=['Source Table Name','SourceOffsetcolumn']))
            
            tgt_generic_df=pd.DataFrame([])
            for tgt_tab_name in maindf['Target Physical Table Name'].unique():
                tgt_off_match=0
                for index,row_tgt in maindf.iterrows():
                    if ((tgt_tab_name.strip().upper() not in ['NA','N/A']) and (str(row_tgt['Target Physical Table Name']).strip().upper()==tgt_tab_name.strip().upper()) and (row_tgt['Offset Column']=='Y')):
                        tgt_generic_df= tgt_generic_df.append(pd.DataFrame([[row_tgt['Target Physical Table Name'],row_tgt['TargetOffsetColumn']]],columns=['Target Physical Table Name','TargetOffsetColumn']))
                        tgt_off_match=1
                        break
                if tgt_off_match==0 and (tgt_tab_name.strip().upper() not in ['NA','N/A']):
                    tgt_generic_df= tgt_generic_df.append(pd.DataFrame([[tgt_tab_name,'']],columns=['Target Physical Table Name','TargetOffsetColumn']))
            
                 
            df_gen_template = pd.read_excel(self.query_template_fname, sheet_name='Query Template',na_filter = False)
            templatedf_generic = df_gen_template[(df_gen_template['Transform Category']=='Generic')]
            df_generic_cat_map = pd.read_excel(self.query_template_fname, sheet_name='Category Mapping', na_filter = False)
            df_generic_cat_map=df_generic_cat_map.filter(items=['Transform Category','Test category'])
            templatedf_generic=pd.merge(templatedf_generic,df_generic_cat_map,how='inner',left_on=['Transform Category','Test case Category'],right_on=['Transform Category','Test category'])
            templatedf_generic=templatedf_generic.drop(columns=['Test category'])
            
            ################ get template for base############
            test_area_type_source=testCaseGenerationConfig.sourceSystemName
            gen_base_tmpl=templatedf_generic[(templatedf_generic['Query Type']=='Base') & (templatedf_generic['Test Area Type']==test_area_type_source)]
                
            ############Get template for compare#########
            test_area_type_target=testCaseGenerationConfig.destinationSystemName
            gen_cmp_tmpl = templatedf_generic[(templatedf_generic['Query Type']=='Compare') & (templatedf_generic['Test Area Type']==test_area_type_target)]
                
            #########Create var-con for baase and compare########
            dfvarOnMap_gen = pd.read_excel(self.query_template_fname, sheet_name='var-con Map', na_filter = False)
            dfvarOnMap_gen = dfvarOnMap_gen[dfvarOnMap_gen['Category']=='Generic']
                
            vardf_basegen=pd.DataFrame(data=None,columns=dfvarOnMap_gen.columns)
            
            for seq in gen_base_tmpl['SequenceNo'].unique():
                    vardf_basegen=vardf_basegen.append(dfvarOnMap_gen[dfvarOnMap_gen['QuerySequenceNo']==seq])            
            
            vardf_comparegen=pd.DataFrame(data=None,columns=dfvarOnMap_gen.columns)
                
            for seq in gen_cmp_tmpl['SequenceNo'].unique():
                vardf_comparegen=vardf_comparegen.append(dfvarOnMap_gen[dfvarOnMap_gen['QuerySequenceNo']==seq])
            
            dfgen_basevar=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
            dfgen_cmpvar=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
               
            for index_map,row_map in src_generic_df.iterrows():
                for index_base,row_base in vardf_basegen.iterrows():
                    if str(row_base['AttributeName']).strip().upper() == 'TABLENAME':
                        dfgen_basevar=dfgen_basevar.append(pd.DataFrame([[row_map['Source Table Name'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],row_map[row_base['TargetValue']]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                    if str(row_base['AttributeName']).strip().upper() == 'OFFSETCOLUMN':
                        dfgen_basevar=dfgen_basevar.append(pd.DataFrame([[row_map['Source Table Name'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],row_map[row_base['TargetValue']]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
            #print dfgen_basevar
            for index_map,row_map in tgt_generic_df.iterrows():
                for index_cmp,row_cmp in vardf_comparegen.iterrows():
                    if str(row_cmp['AttributeName']).strip().upper() == 'TABLENAME':
                        dfgen_cmpvar=dfgen_cmpvar.append(pd.DataFrame([[row_map['Target Physical Table Name'],row_cmp['QuerySequenceNo'],row_cmp['Category'],row_cmp['Query Type'],row_cmp['AttributeType'],row_cmp['ColumnHeader'],row_cmp['AttributeName'],row_map[row_cmp['TargetValue']]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)
                    if str(row_cmp['AttributeName']).strip().upper() == 'OFFSETCOLUMN':
                        dfgen_cmpvar=dfgen_cmpvar.append(pd.DataFrame([[row_map['Target Physical Table Name'],row_cmp['QuerySequenceNo'],row_cmp['Category'],row_cmp['Query Type'],row_cmp['AttributeType'],row_cmp['ColumnHeader'],row_cmp['AttributeName'],row_map[row_cmp['TargetValue']]]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']), ignore_index=True)            
            #print dfgen_cmpvar
            generic_dict = {'basetemp_gendf': pd.DataFrame(gen_base_tmpl),'basevar_gendf': pd.DataFrame(dfgen_basevar),'comparetemp_gendf': pd.DataFrame(gen_cmp_tmpl),'comparevar_gendf': pd.DataFrame(dfgen_cmpvar)}
#             print generic_dict
            self.log.info('*****END=Parse text for Generic')
            gensqlgen=testCaseGenerationTestSQLGenerator.testCaseGenerationTestSQLGenerator()
            genquerydf=gensqlgen.sqlgenerator_for_genericType(generic_dict)
            return genquerydf    
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg
              
    def baseparser_for_TransformType(self,maindf):
        
        try:
            self.log.info('*****START=Parse text for Transform Type')
            maindf['Process Category'] = map(lambda x: x.strip().upper(), maindf['Process Category'])
            maindf=maindf[(maindf['Process Category']=='TRANSFORM') & (maindf['Join Reference']=='')]
            #print maindf
            dftransfm_template = pd.read_excel(self.query_template_fname, sheet_name='Query Template',na_filter = False)
            
            ########## Join and get template###########
            templatedf_transfm=dftransfm_template[(dftransfm_template['Transform Category']=='Simple Transform')]
            df_transfm_cat_map = pd.read_excel(self.query_template_fname, sheet_name='Category Mapping', na_filter = False)
            df_transfm_cat_map=df_transfm_cat_map.filter(items=['Transform Category','Test category'])
            templatedf_transfm=pd.merge(templatedf_transfm,df_transfm_cat_map,how='inner',left_on=['Transform Category','Test case Category'],right_on=['Transform Category','Test category'])
            templatedf_transfm=templatedf_transfm.drop(columns=['Test category'])
            
            ################ get template for base############
            #test_area_type_source='Hive'#Should come from config
            test_area_type_source=testCaseGenerationConfig.sourceSystemName
            dftransfm_basetemplate=templatedf_transfm[(templatedf_transfm['Query Type']=='Base') & (templatedf_transfm['Test Area Type']==test_area_type_source)]
            
            #print dfrdm_basetemplate  
            ############Get template for compare#########
            #test_area_type_target='Hive'#Should come from config
            test_area_type_target=testCaseGenerationConfig.destinationSystemName
            dftransfm_cmptemplate = templatedf_transfm[(templatedf_transfm['Query Type']=='Compare') & (templatedf_transfm['Test Area Type']==test_area_type_target)]
            
            #print dfrdm_basetemplate
            #########Create var-con for baase and compare########
            dfvarOnMap_transfm = pd.read_excel(self.query_template_fname, sheet_name='var-con Map', na_filter = False)
            dfvarOnMap_transfm = dfvarOnMap_transfm[dfvarOnMap_transfm['Category']=='Simple Transform']
            
            vardf_basetransfm=pd.DataFrame(data=None,columns=dfvarOnMap_transfm.columns)
            
            for seq in dftransfm_basetemplate['SequenceNo'].unique():
                vardf_basetransfm=vardf_basetransfm.append(dfvarOnMap_transfm[dfvarOnMap_transfm['QuerySequenceNo']==seq])
           
            
            vardf_comparetransfm=pd.DataFrame(data=None,columns=dfvarOnMap_transfm.columns)
            
            for seq in dftransfm_cmptemplate['SequenceNo'].unique():
                vardf_comparetransfm=vardf_comparetransfm.append(dfvarOnMap_transfm[dfvarOnMap_transfm['QuerySequenceNo']==seq])
               
            dftransfm_basevar=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])
            dftransfm_cmpvar=pd.DataFrame(data=None,columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value'])

            for index_map,row_map in maindf.iterrows():
#                 target_tab_nm=row_map['Target Physical Table Name']
#                 source_tab_nm=row_map['Source table(s)']
                for index_tmpl,row_base in vardf_basetransfm.iterrows():

                    if ((str(row_base['TargetSource']).strip().upper() == 'MAPPING') and (str(row_base['TargetValue']).strip().upper() not in  ['TRANSFORMATION RULE','JOINING CONDITION'])):
                        colval=row_map[row_base['TargetValue']]
                        dftransfm_basevar=dftransfm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],colval]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']))
                    if str(row_base['TargetSource']).strip().upper() == 'HARDCODE':
                        print str(row_base['TargetSource']).strip().upper()
                        dftransfm_basevar=dftransfm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],str(row_base['TargetValue'])]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']))
                    if ((str(row_base['TargetSource']).strip().upper() == 'MAPPING') and (str(row_base['TargetValue']).strip().upper() =='TRANSFORMATION RULE')):
                        if re.search(r"(^CASE WHEN)(.*)(?=THEN)(.*)(?=ELSE)(.*)(?=END)",str(row_map[row_base['TargetValue']]).strip(),re.IGNORECASE)<>None:
                            #Between when...then
                            src_cond_list=re.findall(r"(?<=WHEN)(.*?)(?=THEN)",str(row_map[row_base['TargetValue']]).strip(), re.IGNORECASE)
                        dftransfm_basevar=dftransfm_basevar.append(pd.DataFrame([[row_map['Tracebility ID'],row_base['QuerySequenceNo'],row_base['Category'],row_base['Query Type'],row_base['AttributeType'],row_base['ColumnHeader'],row_base['AttributeName'],src_cond_list]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']))
                
#                 print dftransfm_basevar
                for index_tmpl,row_cmp in vardf_comparetransfm.iterrows():
                    
                    if ((str(row_cmp['TargetSource']).strip().upper() == 'MAPPING') and (str(row_cmp['TargetValue']).strip().upper() not in  ['TRANSFORMATION RULE','JOINING CONDITION'])):
                        colval=row_map[row_cmp['TargetValue']]
                        dftransfm_cmpvar=dftransfm_cmpvar.append(pd.DataFrame([[row_map['Tracebility ID'],row_cmp['QuerySequenceNo'],row_cmp['Category'],row_cmp['Query Type'],row_cmp['AttributeType'],row_cmp['ColumnHeader'],row_cmp['AttributeName'],colval]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']))
                    if str(row_cmp['TargetSource']).strip().upper() == 'HARDCODE':
                        hardcd_val=str(row_cmp['TargetValue']).strip()
                        dftransfm_cmpvar=dftransfm_cmpvar.append(pd.DataFrame([[row_map['Tracebility ID'],row_cmp['QuerySequenceNo'],row_cmp['Category'],row_cmp['Query Type'],row_cmp['AttributeType'],row_cmp['ColumnHeader'],row_cmp['AttributeName'],hardcd_val]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']))
                    if ((str(row_cmp['TargetSource']).strip().upper() == 'MAPPING') and (str(row_cmp['TargetValue']).strip().upper() == 'TRANSFORMATION RULE')):
                        transform_text=str(row_map[row_cmp['TargetValue']]).strip().upper()
#                         print transform_text
                        tgt_cond_list=[]
                        if re.search(r"(^CASE WHEN)(.*)(?=THEN)(.*)(?=ELSE)(.*)(?=END)",transform_text,re.IGNORECASE)<>None:
                            if re.search(r"(?<=THEN)(.*?)(?=WHEN)",transform_text, re.IGNORECASE) <> None:
                                #Between then...when
                                tgt_cond_list=re.findall(r"(?<=THEN)(.*?)(?=WHEN)",transform_text, re.IGNORECASE)
                            if re.search(r"(?<=(THEN)(?!.*THEN))(.*?)(?=ELSE)",transform_text, re.IGNORECASE) <> None:
                                #Betwen then...else
                                tgt_cond_list.append(re.findall(r"(?<=(THEN)(?!.*THEN))(.*?)(?=ELSE)",transform_text, re.IGNORECASE)[-1][-1])
                            if re.search(r"(?<=ELSE)(.*?)(?=END)",transform_text, re.IGNORECASE) <> None:
                                #Between else....end
                                tgt_cond_list.append(re.findall(r"(?<=ELSE)(.*?)(?=END)",transform_text, re.IGNORECASE)[-1])
                        dftransfm_cmpvar=dftransfm_cmpvar.append(pd.DataFrame([[row_map['Tracebility ID'],row_cmp['QuerySequenceNo'],row_cmp['Category'],row_cmp['Query Type'],row_cmp['AttributeType'],row_cmp['ColumnHeader'],row_cmp['AttributeName'],tgt_cond_list]],columns=['Tracebility ID','QuerySequenceNo','Category','Query Type','AttributeType','ColumnHeader','AttributeName','Value']))
                    
#                 print dftransfm_cmpvar
            transform_dict = {'basetemp_transfmdf': pd.DataFrame(dftransfm_basetemplate),'basevar_transfmdf': pd.DataFrame(dftransfm_basevar),'comparetemp_transfmdf': pd.DataFrame(dftransfm_cmptemplate),'comparevar_transfmdf': pd.DataFrame(dftransfm_cmpvar)}
#             print transform_dict
            self.log.info('*****END=Parse text for Transform Type')
            transfmsqlgen=testCaseGenerationTestSQLGenerator.testCaseGenerationTestSQLGenerator()
            transfmquerydf=transfmsqlgen.sqlgenerator_for_TransformType(transform_dict)
            return transfmquerydf
        except Exception as e:
            exceptionmsg= self.errhandler.check_Technical_Exception(e)
            self.log.error(exceptionmsg)
            print exceptionmsg
# test = testCaseGenerationBaseParser()
# maindf=pd.DataFrame([])
# test.baseparser_for_defaultType_final(maindf)     