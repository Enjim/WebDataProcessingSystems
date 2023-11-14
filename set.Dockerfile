FROM python:3.9.12

WORKDIR /opt

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY __init__.py __init__.py
COPY final_EL.py final_EL.py
COPY final_RE.py final_RE.py
COPY parse_ner.py parse_ner.py
COPY info_extract.py info_extract.py
COPY evaluation.py evaluation.py
COPY data/warcs/sample.warc.gz opt/data/warcs/sample.warc.gz


CMD [ "python", "info_extract.py"]
# test = y,n
#input = opt/{local DIR of data}