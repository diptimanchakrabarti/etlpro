'''
Created on Nov 17, 2017

@author: ibm
'''
from flask import Flask, flash, render_template,request,send_file,redirect,url_for
import os
import yaml
from os import walk
from mainController import testCaseGenerationConfig
# import mainController
from mainController import testCaseGenerationOrchestrator as tcgto
from werkzeug.utils import secure_filename
from baseUtils import testCaseGenerationLogger
from baseUtils import testCaseGenerationErrorHandler

logger =   testCaseGenerationLogger.testCaseGenerationLogger()
log = logger.getLogger('testCaseGenerationSimpleUI')
errhandler=testCaseGenerationErrorHandler.testCaseGenerationErrorHandler()

app = Flask(__name__,static_url_path='/templates', static_folder='templates')
ALLOWED_EXTENSIONS = set(['xls', 'xlsx'])
# download_file_name=''

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload')
def home():
#    return render_template('file_upload.html',user_image=imagefile)
    return render_template('file_upload.html')#redirect(url_for('upload_file'))

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
                    print "filename:" + download_file_name
                    testCaseGenerationConfig.download_filename=download_file_name
                    return redirect(url_for('download_files'))
        return render_template('file_upload.html')
    except Exception as e:
        exceptionmsg=errhandler.check_Business_Exception(str(e))
        log.error(exceptionmsg)
#         flash(exceptionmsg)
        return render_template('file_upload.html')

@app.route('/download',methods = ['GET','POST'])
def download_files():
    if request.method == 'GET':
        return render_template('file_download.html')

@app.route('/return-files',methods = ['GET','POST'])
def return_files():
    if request.method == 'POST':
        output_dir=testCaseGenerationConfig.testcase_destination_path
        app.config['DOWNLOAD_FOLDER'] = output_dir
	filePathName=testCaseGenerationConfig.download_filename
	print "The filePathName is:", filePathName
#         return send_file(os.path.join(app.config['DOWNLOAD_FOLDER'], 'TestScript_Final.xlsx'),attachment_filename='TestScript_Final.xlsx',as_attachment=True)
        return send_file(os.path.abspath(filePathName),as_attachment=True)
#     #return send_file(os.path.join(app.config['DOWNLOAD_FOLDER'], 'TestScript_Final.xlsx'),attachment_filename='TestScript_Final.xlsx')

def sortdict(d):
    for key in sorted(d,reverse=True)[:10]: yield d[key]

def pick_log_file_names():
    mypath=testCaseGenerationConfig.log_file_path
    f = []
    filelist = []
    for (dirpath, dirnames, filenames) in walk(mypath):
        f.extend(filenames)
        break
    dicts = {}
    for i in f:
    #print (i[0:10])
        dicts[i[0:10]] = i    
    for value in sortdict(dicts):
        filelist.append(value)
    return filelist   


@app.route('/download_logs',methods = ['GET','POST'])
def download_logs():
    if request.method == 'POST':
#         log_filename=str(request.form.get('download')).split(' ')[-1]
#         print request.form.get('download')
#         return send_file(os.path.join('C:\\Users\\IBM_ADMIN\\workspacepython\\testCaseGeneratorUtilProject\\src\\logs',log_filename),attachment_filename=log_filename,as_attachment=True)
        if request.form.get('download') == 'Download Log':
            log_filepath=testCaseGenerationConfig.log_file_path
            log_filename=request.form["option"]
            return send_file(os.path.abspath(os.path.join(log_filepath,log_filename)),attachment_filename=log_filename,as_attachment=True)
              
    newfilelist = pick_log_file_names()
    return render_template('log_download.html', log_list=newfilelist)



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
