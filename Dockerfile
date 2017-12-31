FROM python:2

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt


COPY . ./root/

ENV PYTHONPATH=./root/src

CMD ["python", "./root/src/mainController/testCaseGenerationSimpleUI.py"]



