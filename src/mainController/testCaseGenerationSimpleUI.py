'''
Created on Nov 17, 2017

@author: ibm
'''
from flask import Flask, flash, render_template,request,send_file,redirect,url_for
import os
import yaml
from mainController import testCaseGenerationConfig
# import mainController
from mainController import testCaseGenerationOrchestrator as tcgto
from werkzeug.utils import secure_filename
from baseUtils import testCaseGenerationLogger
from baseUtils import testCaseGenerationErrorHandler


app = Flask(__name__, static_url_path='/templates', static_folder='templates')
log=''
errhandler=''

ALLOWED_EXTENSIONS = set(['xls', 'xlsx'])
# download_file_name=''

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS                 

@app.route('/upload')
def home():
#    return render_template('file_upload.html',user_image=imagefile)
    return render_template('file_upload.html')#redirect(url_for('upload_file'))
    logger =   testCaseGenerationLogger.testCaseGenerationLogger()
    log = logger.getLogger('testCaseGenerationSimpleUI')
    errhandler=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()

@app.route('/uploaded',methods = ['GET','POST'])
def upload_file():
   try: 
        if request.method == 'POST':
            src_select = str(request.form.get('source_select')).strip()
            tgt_select = str(request.form.get('target_select')).strip()
            if src_select <> "None" and tgt_select <> "None":
                if 'file' not in request.files:
                    flash('No file part')
                    return redirect(url_for('home'))
                file=request.files['file']
                if file.filename == None:
                    flash('No selected file')
                    return redirect(url_for('home'))
                if file and allowed_file(file.filename):
        #             filename = secure_filename(file.filename)
    #                 print fname
                    ####### Update Config ########
    #                 mainController.testCaseGenerationConfig.update_config(src_select,tgt_select)
                    testCaseGenerationConfig.update_config(src_select,tgt_select)
                    print "Config updated"
                    ##############################
                    upload_folder=testCaseGenerationConfig.maping_path
                    app.config['UPLOAD_FOLDER'] = upload_folder
                    filename = secure_filename(file.filename)
                    file.save(os.path.join(app.config['UPLOAD_FOLDER'],filename))
                    fname = os.path.join(app.config['UPLOAD_FOLDER'],filename)
                    
                    tcgtorch=tcgto.testCaseGenerationOrchestartor()
                    download_file_name=tcgtorch.reader_orchestrator(fname)
                    testCaseGenerationConfig.download_filename=download_file_name
                    return redirect(url_for('download_files'))
        return redirect(url_for('home'))
   except Exception as e:  
                     exceptionmsg=errhandler.check_Technical_Exception(e)
                     log.error(exceptionmsg)    
                     exit()
@app.route('/download',methods = ['GET','POST'])
def download_files():
    if request.method == 'GET':
        return render_template('file_download.html')

@app.route('/return-files',methods = ['GET','POST'])
def return_files():
    if request.method == 'POST':
        output_dir=testCaseGenerationConfig.testcase_destination_path
        app.config['DOWNLOAD_FOLDER'] = output_dir
#         return send_file(os.path.join(app.config['DOWNLOAD_FOLDER'], 'TestScript_Final.xlsx'),attachment_filename='TestScript_Final.xlsx',as_attachment=True)
        return send_file(os.path.join(app.config['DOWNLOAD_FOLDER'],str(testCaseGenerationConfig.download_filename).split("/")[-1]),attachment_filename=str(testCaseGenerationConfig.download_filename).split("/")[-1],as_attachment=True)
#     #return send_file(os.path.join(app.config['DOWNLOAD_FOLDER'], 'TestScript_Final.xlsx'),attachment_filename='TestScript_Final.xlsx')

if __name__ == '__main__':
    app.run(debug=True) 