import testCaseGenerationErrorHandler 





class test(object):

   def __init__(self):
        pass
        
       
   def testExceptionmethod1(self):
       errhandler = testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()
       try:
            print 1/0
       except Exception as e:
            #exceptionmsg = errhandler.checkTechnicalException(e)
            exceptionmsg1 = errhandler.checkBusinessException('Mapping_Doc_Error')   
             #print "You can't divide by zero, you're silly."
            print exceptionmsg1
            #print exceptionmsg
            
             
a = test()
a.testExceptionmethod1()             
     
     
    