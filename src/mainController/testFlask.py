'''
Created on Nov 17, 2017

@author: ibm
'''
from flask import Flask, render_template,request
import os
upload_folder='C:\\Users\\IBM_ADMIN\\workspacepython\\testCaseGeneratorUtilProject\\src\\mainController\\Uploaded'
import testCaseGenerationOrchestrator as tcgto

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = upload_folder

@app.route('/upload')
def fileupload():
    return render_template('file_upload.html')

@app.route('/uploaded',methods = ['GET','POST'])
def home():
    if request.method == 'POST':
        f=request.files['file']
        f.save(os.path.join(app.config['UPLOAD_FOLDER'],f.filename))
        fname = os.path.join(app.config['UPLOAD_FOLDER'],f.filename)
        #print fname
        tcgto.readXls(fname)
        return "File Uploaded successfully!"
        
if __name__ == '__main__':
    app.run()