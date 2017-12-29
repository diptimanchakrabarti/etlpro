'''
Created on Nov 9, 2017

@author: IBM
'''
import pandas as pd
from baseUtils import testCaseGenerationLogger
from baseUtils import testCaseGenerationErrorHandler

class testCaseGenerationTestSQLGenerator(object):

      def __init__(self):
          logger =   testCaseGenerationLogger.testCaseGenerationLogger()
          self.log = logger.getLogger(self.__class__.__name__)
          #self.log.info('This is logger for testCaseGenerationFileConnector:init method')
          self.errhandler=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
          
          self.test_case_description_dic = {
                        "Count Match - Column" : "match row count in source and Destination",
                        "Value Match - Column" : "Match the number of values in source and destination",
                        "Count Match - Table" : "match row count in source and Destination"
                        }
                    
          
            #This is Test SQL Query Generation function for Straight - Base + Compare    
      def SQLGeneratorforStraightType(self,default_dict):
         try:  
            self.log.info('*****START=Generating Queries for Straight Type')
            comparedf_straight=pd.DataFrame([])
            basedf_straight=pd.DataFrame([])
            #This loop will prepare Dataframes for Default-Base + Default-Compare Cases  
            for k,v in default_dict.iteritems(): 
               if k=='basetemp_straightdf':
                   interim_templatedf_base_straight=v
                   #print interim_templatedf_base_straight
               if k=='basevar_straightdf':
                   final_vardf_base_straight=v
                   #print final_vardf_base_straight
               if k=='comparetemp_straightdf':
                   interim_templatedf_compare_straight=v
                   #print interim_templatedf_compare_straight
               if k=='comparevar_straightdf':            
                    final_vardf_comapre_straight=v
                    #print final_vardf_comapre_straight
            #print final_vardf_base_straight
            final_straight=pd.DataFrame([])
            #if not (interim_templatedf_base_straight.empty and final_vardf_comapre_straight.empty):
                #Need to check for and/or
            sub_start_point=int(interim_templatedf_base_straight.columns.get_loc('Sub1'))
            #print sub_start_point
            #print interim_templatedf_compare_straight
            #print interim_templatedf_base_straight            
            #basedf_straight=pd.DataFrame(data=None,columns=['Tracebility ID','TestCase','TargetXLSTab','ProcessCategory','Query Type'], ignore_index=True)
        
            basedflen=int(len(interim_templatedf_base_straight.columns))
            #print basedflen
            for trace_id in final_vardf_base_straight['Tracebility ID'].unique():
                #print trace_id
                for index_tmp,row_tmp in interim_templatedf_base_straight.iterrows():
                    #print row_tmp[0]
                    qstr_base_straight=''
                    final_vardf_base_straight_trace=final_vardf_base_straight[final_vardf_base_straight['Tracebility ID']==trace_id]
                    for ctr in range(0,basedflen-sub_start_point):
                        match=0
                        for index_var,row_var in final_vardf_base_straight_trace.iterrows():
                            if (row_var['QuerySequenceNo']==row_tmp[0]) and (row_tmp[ctr+sub_start_point]<>'') and (str(row_tmp[ctr+sub_start_point]).strip().upper()==str(row_var['AttributeName']).strip().upper()):
                                qstr_base_straight = qstr_base_straight + ' ' + str(row_var['Value']).strip()
                                match=1
                                break
                        if match==0:
                            qstr_base_straight=qstr_base_straight + ' ' + str(row_tmp[ctr+sub_start_point]).strip()
                    #print qstr_base_straight
                    basedf_straight=basedf_straight.append(pd.DataFrame([[trace_id,qstr_base_straight,'SOURCE','STRAIGHT','BASE',row_tmp['Test case Category'],row_tmp['Expected Result'],row_tmp['Test Area Type'],self.test_case_description_dic[row_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)      
            basecmplen=int(len(interim_templatedf_compare_straight.columns))
            for trace_cmp_id in final_vardf_comapre_straight['Tracebility ID'].unique():
#                 print trace_cmp_id              
                for index,row_cmp_tmp in interim_templatedf_compare_straight.iterrows():
                    qstr_cmp_straight=''
                    final_vardf_cmp_straight_trace=final_vardf_comapre_straight[final_vardf_comapre_straight['Tracebility ID']==trace_cmp_id]
                    #print final_vardf_cmp_straight_trace 
                    for ctr_cmp in range(0,basecmplen-sub_start_point):
                        cmp_match=0
                        for index,row_cmp_var in final_vardf_cmp_straight_trace.iterrows():
                            if (row_cmp_var['QuerySequenceNo']==row_cmp_tmp[0]) and (row_cmp_tmp[ctr_cmp+sub_start_point]<>'') and (str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip().upper()==str(row_cmp_var['AttributeName']).strip().upper()):
                                qstr_cmp_straight = qstr_cmp_straight + ' ' + str(row_cmp_var['Value']).strip()
                                cmp_match=1
                                break
                        if cmp_match==0:
                            qstr_cmp_straight=qstr_cmp_straight + ' ' + str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip()

                    comparedf_straight=comparedf_straight.append(pd.DataFrame([[trace_cmp_id,qstr_cmp_straight,'TARGET','STRAIGHT','COMPARE',row_cmp_tmp['Test case Category'],row_cmp_tmp['Expected Result'],row_cmp_tmp['Test Area Type'],self.test_case_description_dic[row_cmp_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)

            final_straight = basedf_straight.append(comparedf_straight,ignore_index=True)
            self.log.info('*****END=Completed generating Queries for Straight Type')
    #             print final_straight
            return final_straight
            #return basedf_straight        
         except Exception as e:
              exceptionmsg=self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg 
                 
      #This is Test SQL Query Generation function for Default - Base + Compare    
      def SQLGeneratorforDefaultType(self,default_dict):
        try:  
            self.log.info('*****START=Generating Queries for Default Type')
            comparedf_default=pd.DataFrame([])
            basedf_default=pd.DataFrame([])
            #This loop will prepare Dataframes for Default-Base + Default-Compare Cases  
            for k,v in default_dict.iteritems(): 
               if k=='basetemp_defaultdf':
                   interim_templatedf_base_default=v
                   #print interim_templatedf_base_straight
               if k=='basevar_defaultdf':
                   final_vardf_base_default=v
                   #print final_vardf_base_straight
               if k=='comparetemp_defaultdf':
                   interim_templatedf_compare_default=v
                   #print interim_templatedf_compare_straight
               if k=='comparevar_defaultdf':            
                    final_vardf_comapre_default=v
                    #print final_vardf_comapre_straight
            #print final_vardf_base_straight
            final_default=pd.DataFrame([])
#             if not(final_vardf_base_default.empty and final_vardf_comapre_default.empty):
                #Need to check for and/or
            sub_start_point=int(interim_templatedf_base_default.columns.get_loc('Sub1'))
            #print sub_start_point
            #print interim_templatedf_compare_straight
            #print interim_templatedf_base_straight            
            #basedf_straight=pd.DataFrame(data=None,columns=['Tracebility ID','TestCase','TargetXLSTab','ProcessCategory','Query Type'], ignore_index=True)
            basedf_default=pd.DataFrame([])
            comparedf_default=pd.DataFrame([])
            basedflen=int(len(interim_templatedf_base_default.columns))
            #print basedflen
            for trace_id in final_vardf_base_default['Tracebility ID'].unique():
                #print trace_id
                for index_tmp,row_tmp in interim_templatedf_base_default.iterrows():
                    #print row_tmp[0]
                    qstr_base_default=''
                    final_vardf_base_default_trace=final_vardf_base_default[final_vardf_base_default['Tracebility ID']==trace_id]
                    for ctr in range(0,basedflen-sub_start_point):
                        match=0
                        for index_var,row_var in final_vardf_base_default_trace.iterrows():
                            if (row_var['QuerySequenceNo']==row_tmp[0]) and (row_tmp[ctr+sub_start_point]<>'') and (str(row_tmp[ctr+sub_start_point]).strip().upper()==str(row_var['AttributeName']).strip().upper()):
                                qstr_base_default = qstr_base_default + ' ' + str(row_var['Value']).strip()
                                match=1
                                break
                        if match==0:
                            qstr_base_default=qstr_base_default + ' ' + str(row_tmp[ctr+sub_start_point]).strip()
                    #print qstr_base_straight
                    basedf_default=basedf_default.append(pd.DataFrame([[trace_id,qstr_base_default,'TARGET','DEFAULT','BASE',row_tmp['Test case Category'],row_tmp['Expected Result'],row_tmp['Test Area Type'],self.test_case_description_dic[row_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)      
            basecmplen=int(len(interim_templatedf_compare_default.columns))
            for trace_cmp_id in final_vardf_comapre_default['Tracebility ID'].unique():
#                 print trace_cmp_id              
                for index,row_cmp_tmp in interim_templatedf_compare_default.iterrows():
                    qstr_cmp_default=''
                    final_vardf_comapre_default_trace=final_vardf_comapre_default[final_vardf_comapre_default['Tracebility ID']==trace_cmp_id]
                    #print final_vardf_cmp_straight_trace 
                    for ctr_cmp in range(0,basecmplen-sub_start_point):
                        cmp_match=0
                        for index,row_cmp_var in final_vardf_comapre_default_trace.iterrows():
#                             print row_cmp_var['QuerySequenceNo']
                            if (row_cmp_var['QuerySequenceNo']==row_cmp_tmp[0]) and (row_cmp_tmp[ctr_cmp+sub_start_point]<>'') and (str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip().upper()==str(row_cmp_var['AttributeName']).strip().upper()):
                                qstr_cmp_default = qstr_cmp_default + ' ' + str(row_cmp_var['Value']).strip()
                                cmp_match=1
                                break
                        if cmp_match==0:
                            qstr_cmp_default=qstr_cmp_default + ' ' + str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip()

                    comparedf_default=comparedf_default.append(pd.DataFrame([[trace_cmp_id,qstr_cmp_default,'TARGET','DEFAULT','COMPARE',row_cmp_tmp['Test case Category'],row_cmp_tmp['Expected Result'],row_cmp_tmp['Test Area Type'],self.test_case_description_dic[row_cmp_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)
            
            final_default = basedf_default.append(comparedf_default,ignore_index=True)
            self.log.info('*****END=Completed generating Queries for Default Type')
#             print final_default
            return final_default
            #return basedf_straight        
        except Exception as e:
              exceptionmsg=self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg

        ##########################################################################
        #RDM JOIN TYPE SQL GEN FUNCTION
        ##########################################################################
        
      def sqlgenerator_for_rdmjoinType(self,rdm_dict):
        try:  
            self.log.info('*****START=Generating Queries for rdmjoinType')
            comparedf_rdm=pd.DataFrame([])
            basedf_rdm=pd.DataFrame([])
            for k,v in rdm_dict.iteritems(): 
                if k=='basetemp_rdmdf':
                    interim_templatedf_base_rdm=v
                    #print interim_templatedf_base_straight
                if k=='basevar_rdmdf':
                    final_vardf_base_rdm=v
                if k=='comparetemp_rdmdf':
                    interim_templatedf_compare_rdm=v
                if k=='comparevar_rdmdf':            
                    final_vardf_compare_rdm=v
            
            final_rdm=pd.DataFrame([])
#             if not (final_vardf_base_rdm.empty and final_vardf_compare_rdm.empty):            
                #Need to check for and/or
            sub_start_point=int(interim_templatedf_base_rdm.columns.get_loc('Sub1'))
            basedflen=int(len(interim_templatedf_base_rdm.columns))

            for trace_id in final_vardf_base_rdm['Tracebility ID'].unique():
                #print trace_id
                for index_tmp,row_tmp in interim_templatedf_base_rdm.iterrows():
                    #print row_tmp[0]
                    qstr_base_straight=''
                    final_vardf_base_rdm_trace=final_vardf_base_rdm[final_vardf_base_rdm['Tracebility ID']==trace_id]
                    for ctr in range(0,basedflen-sub_start_point):
                        match=0
                        for index_var,row_var in final_vardf_base_rdm_trace.iterrows():
                            if (row_var['QuerySequenceNo']==row_tmp[0]) and (row_tmp[ctr+sub_start_point]<>'') and (str(row_tmp[ctr+sub_start_point]).strip().upper()==str(row_var['AttributeName']).strip().upper()):
                                qstr_base_straight = qstr_base_straight + ' ' + str(row_var['Value']).strip()
                                match=1
                                break
                        if match==0:
                            qstr_base_straight=qstr_base_straight + ' ' + str(row_tmp[ctr+sub_start_point]).strip()
                    #print qstr_base_straight
                    basedf_rdm=basedf_rdm.append(pd.DataFrame([[trace_id,qstr_base_straight,'SOURCE','RDM JOIN TRANSFORM','BASE',row_tmp['Test case Category'],row_tmp['Expected Result'],row_tmp['Test Area Type'],self.test_case_description_dic[row_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)
            cmpdflen=int(len(interim_templatedf_compare_rdm.columns))

            for trace_cmp_id in final_vardf_compare_rdm['Tracebility ID'].unique():
#                 print trace_cmp_id              
                for index,row_cmp_tmp in interim_templatedf_compare_rdm.iterrows():
                    qstr_cmp_straight=''
                    final_vardf_cmp_rdm_trace=final_vardf_compare_rdm[final_vardf_compare_rdm['Tracebility ID']==trace_cmp_id]
                    #print final_vardf_cmp_straight_trace 
                    for ctr_cmp in range(0,cmpdflen-sub_start_point):
                        cmp_match=0
                        for index,row_cmp_var in final_vardf_cmp_rdm_trace.iterrows():
#                             print row_cmp_var['QuerySequenceNo']
                            if (row_cmp_var['QuerySequenceNo']==row_cmp_tmp[0]) and (row_cmp_tmp[ctr_cmp+sub_start_point]<>'') and (str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip().upper()==str(row_cmp_var['AttributeName']).strip().upper()):
                                qstr_cmp_straight = qstr_cmp_straight + ' ' + str(row_cmp_var['Value']).strip()
                                cmp_match=1
                                break
                        if cmp_match==0:
                            qstr_cmp_straight=qstr_cmp_straight + ' ' + str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip()

                    comparedf_rdm=comparedf_rdm.append(pd.DataFrame([[trace_cmp_id,qstr_cmp_straight,'TARGET','RDM JOIN TRANSFORM','COMPARE',row_cmp_tmp['Test case Category'],row_cmp_tmp['Expected Result'],row_cmp_tmp['Test Area Type'],self.test_case_description_dic[row_cmp_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)
    
            final_rdm = basedf_rdm.append(comparedf_rdm,ignore_index=True)
                #print final_rdm

            self.log.info('*****END=Completed generating Queries for rdmjoinType')
            return final_rdm      
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg
              
      def sqlgenerator_for_genericType(self,gen_dict):
        try:  
            self.log.info('*****START=Generating Queries for generic Type')
            comparedf_rdm=pd.DataFrame([])
            basedf_rdm=pd.DataFrame([])
            for k,v in gen_dict.iteritems(): 
                if k=='basetemp_gendf':
                    interim_templatedf_base_gen=v
                if k=='basevar_gendf':
                    final_vardf_base_gen=v
                if k=='comparetemp_gendf':
                    interim_templatedf_compare_gen=v
                if k=='comparevar_gendf':            
                    final_vardf_compare_gen=v
            
            final_gen=pd.DataFrame([])
            
            sub_start_point=int(interim_templatedf_base_gen.columns.get_loc('Sub1'))
            basedflen=int(len(interim_templatedf_base_gen.columns))
            
            basedf_gen=pd.DataFrame([])
            comparedf_gen=pd.DataFrame([])
                       
            for trace_id in final_vardf_base_gen['Tracebility ID'].unique():
                #print trace_id
                for index_tmp,row_tmp in interim_templatedf_base_gen.iterrows():
                    qstr_base_gen=''
                    final_vardf_base_gen_trace=final_vardf_base_gen[final_vardf_base_gen['Tracebility ID']==trace_id]
                    for ctr in range(0,basedflen-sub_start_point):
                        match=0
                        for index_var,row_var in final_vardf_base_gen_trace.iterrows():
                            if (row_var['QuerySequenceNo']==row_tmp[0]) and (row_tmp[ctr+sub_start_point]<>'') and (str(row_tmp[ctr+sub_start_point]).strip().upper()==str(row_var['AttributeName']).strip().upper()):
                                qstr_base_gen = qstr_base_gen + ' ' + str(row_var['Value']).strip()
                                match=1
                                break
                        if match==0:
                            qstr_base_gen=qstr_base_gen + ' ' + str(row_tmp[ctr+sub_start_point]).strip()

                    basedf_gen=basedf_gen.append(pd.DataFrame([[trace_id,qstr_base_gen,'SOURCE','GENERIC','BASE',row_tmp['Test case Category'],row_tmp['Expected Result'],row_tmp['Test Area Type'],self.test_case_description_dic[row_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)

            cmpdflen=int(len(interim_templatedf_compare_gen.columns))

            for trace_cmp_id in final_vardf_compare_gen['Tracebility ID'].unique():
#                 print trace_cmp_id              
                for index,row_cmp_tmp in interim_templatedf_compare_gen.iterrows():
                    qstr_cmp_gen=''
                    final_vardf_cmp_gen_trace=final_vardf_compare_gen[final_vardf_compare_gen['Tracebility ID']==trace_cmp_id]
                    #print final_vardf_cmp_straight_trace 
                    for ctr_cmp in range(0,cmpdflen-sub_start_point):
                        cmp_match=0
                        for index,row_cmp_var in final_vardf_cmp_gen_trace.iterrows():
#                             print row_cmp_var['QuerySequenceNo']
                            if (row_cmp_var['QuerySequenceNo']==row_cmp_tmp[0]) and (row_cmp_tmp[ctr_cmp+sub_start_point]<>'') and (str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip().upper()==str(row_cmp_var['AttributeName']).strip().upper()):
                                qstr_cmp_gen = qstr_cmp_gen + ' ' + str(row_cmp_var['Value']).strip()
                                cmp_match=1
                                break
                        if cmp_match==0:
                            qstr_cmp_gen=qstr_cmp_gen + ' ' + str(row_cmp_tmp[ctr_cmp+sub_start_point]).strip()

                    comparedf_gen=comparedf_gen.append(pd.DataFrame([[trace_cmp_id,qstr_cmp_gen,'TARGET','GENERIC','COMPARE',row_cmp_tmp['Test case Category'],row_cmp_tmp['Expected Result'],row_cmp_tmp['Test Area Type'],self.test_case_description_dic[row_cmp_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)
    
            final_gen = basedf_gen.append(comparedf_gen,ignore_index=True)
                #print final_rdm

            self.log.info('*****END=Completed generating Queries for generic Type')
            return final_gen      
        except Exception as e:
              exceptionmsg= self.errhandler.check_Technical_Exception(e)
              self.log.error(exceptionmsg)
              print exceptionmsg
              
      def sqlgenerator_for_TransformType(self,transfm_dict):
        try:  
            self.log.info('*****START=Generating Queries for Transform Type')
            comparedf_transfm=pd.DataFrame([])
            basedf_transfm=pd.DataFrame([])
            for k,v in transfm_dict.iteritems(): 
                if k=='basetemp_transfmdf':
                    interim_templatedf_base_transfm=v
                if k=='basevar_transfmdf':
                    final_vardf_base_transfm=v
                if k=='comparetemp_transfmdf':
                    interim_templatedf_compare_transfm=v
                if k=='comparevar_transfmdf':            
                    final_vardf_compare_transfm=v
                     
            final_rdm=pd.DataFrame([])
 
            sub_start_point=int(interim_templatedf_base_transfm.columns.get_loc('Sub1'))
            basedflen=int(len(interim_templatedf_base_transfm.columns))

            for trace_id in final_vardf_base_transfm['Tracebility ID'].unique():

                for index_tmp,row_tmp in interim_templatedf_base_transfm.iterrows():
                    final_vardf_base_transfm_trace=final_vardf_base_transfm[final_vardf_base_transfm['Tracebility ID']==trace_id]
                    final_vardf_base_transfm_trace=final_vardf_base_transfm_trace.reset_index(drop=True)
                    
                    for index,row_trace in final_vardf_base_transfm_trace.iterrows():
                        if str(row_trace['AttributeName']).strip()=='condition1':
                            cond_list=row_trace['Value']
                            break
#                     print cond_list
                    final_cond=' OR '.join(cond_list)                    
                    cond_list.append(self.transfm_char_replace(final_cond))

                    for cond in cond_list:
                        qstr_base_transfm=''
                        indx=final_vardf_base_transfm_trace[final_vardf_base_transfm_trace['AttributeName']=='condition1'].index[0]

                        final_vardf_base_transfm_trace.ix[indx,final_vardf_base_transfm_trace.columns.get_loc('Value')]=cond
                        for ctr in range(0,basedflen-sub_start_point):
                            match=0
                            for index_var,row_var in final_vardf_base_transfm_trace.iterrows():
                                if (row_var['QuerySequenceNo']==row_tmp[0]) and (row_tmp[ctr+sub_start_point]<>'') and (str(row_tmp[ctr+sub_start_point]).strip().upper()==str(row_var['AttributeName']).strip().upper()):
#                                     print str(row_var['Value']).strip()
                                    qstr_base_transfm = qstr_base_transfm + ' ' + str(row_var['Value']).strip()
                                    match=1
                                    break
                            if match==0:
                                qstr_base_transfm=qstr_base_transfm + ' ' + str(row_tmp[ctr+sub_start_point]).strip()
                        basedf_transfm=basedf_transfm.append(pd.DataFrame([[trace_id,qstr_base_transfm,'SOURCE','SIMPLE TRANSFORM','BASE',row_tmp['Test case Category'],row_tmp['Expected Result'],row_tmp['Test Area Type'],self.test_case_description_dic[row_tmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)
            
            cmpdflen=int(len(interim_templatedf_compare_transfm.columns))
            
            for trace_id_cmp in final_vardf_compare_transfm['Tracebility ID'].unique():
                
                for index_tmp,row_tmp_cmp in interim_templatedf_compare_transfm.iterrows():
                    final_vardf_cmp_transfm_trace=final_vardf_compare_transfm[final_vardf_compare_transfm['Tracebility ID']==trace_id_cmp]
                    final_vardf_cmp_transfm_trace=final_vardf_cmp_transfm_trace.reset_index(drop=True)

                    for index,row_trace_cmp in final_vardf_cmp_transfm_trace.iterrows():
                        if str(row_trace_cmp['AttributeName']).strip()=='condition2':
                            cond_list_cmp=row_trace_cmp['Value']
                            break

                    for cond_cmp in cond_list_cmp:
                        qstr_cmp_transfm=''
                        indx=final_vardf_cmp_transfm_trace[final_vardf_cmp_transfm_trace['AttributeName']=='condition2'].index[0]

                        final_vardf_cmp_transfm_trace.ix[indx,final_vardf_cmp_transfm_trace.columns.get_loc('Value')]=cond_cmp

                        for ctr_cmp in range(0,cmpdflen-sub_start_point):
                            match=0
                            for index_var_cmp,row_var_cmp in final_vardf_cmp_transfm_trace.iterrows():
                                if (row_var_cmp['QuerySequenceNo']==row_tmp_cmp[0]) and (row_tmp_cmp[ctr_cmp+sub_start_point]<>'') and (str(row_tmp_cmp[ctr_cmp+sub_start_point]).strip().upper()==str(row_var_cmp['AttributeName']).strip().upper()):
#                                     print str(row_var_cmp['Value']).strip()
                                    qstr_cmp_transfm = qstr_cmp_transfm + ' ' + str(row_var_cmp['Value']).strip()
                                    match=1
                                    break
                            if match==0:
                                qstr_cmp_transfm=qstr_cmp_transfm + ' ' + str(row_tmp_cmp[ctr_cmp+sub_start_point]).strip()
                        comparedf_transfm=comparedf_transfm.append(pd.DataFrame([[trace_id_cmp,qstr_cmp_transfm,'TARGET','SIMPLE TRANSFORM','COMPARE',row_tmp_cmp['Test case Category'],row_tmp_cmp['Expected Result'],row_tmp_cmp['Test Area Type'],self.test_case_description_dic[row_tmp_cmp['Test case Category']]]],columns=['Mapping Reference Number','Action','TargetXLSTab','ProcessCategory','Query Type','Test case Category','Expected Result','Query Execution Area','Test Case Description']), ignore_index=True)                        
            
            
            final_transfm = basedf_transfm.append(comparedf_transfm,ignore_index=True)
            self.log.info('*****END=Completed generating Queries for Transform Type')
            return final_transfm
        except Exception as e:
            exceptionmsg= self.errhandler.check_Technical_Exception(e)
            self.log.error(exceptionmsg)
            print exceptionmsg           
      
      def transfm_char_replace(self,cond_str):
        replace_dict={'=' :'!=','!=':'=',' IN ':' NOT IN ',' NOT IN ': ' IN ','<':'>','>':'<','<=':'>','>=':'<'}
        for k,v in replace_dict.iteritems():
          cond_str=cond_str.replace(k,v)
        cond_str= "("+cond_str+")"
        return cond_str 
                                            