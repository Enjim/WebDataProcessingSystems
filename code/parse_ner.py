from warcio.recordloader import ArcWarcRecordLoader
from lxml import etree
import lxml.html as lh
from lxml.html.clean import Cleaner
from bs4 import BeautifulSoup
from trafilatura import extract
import os
from tqdm import tqdm
import re
import spacy
import gzip
from bs4 import BeautifulSoup
import os
import subprocess
print("*** Downloading Spacy NER language module ***")
subprocess.run(["python","-m","spacy","download","en_core_web_lg"])
nlp = spacy.load("en_core_web_lg")
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor




def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload

def parse(row):
    record = ArcWarcRecordLoader()
    record = record.parse_record_stream(row, known_format="warc")
    record = record.content_stream().read()
    return {"warc": record, "raw": str(row) }

def clean_html(html):
    cleaner = Cleaner(page_structure=False, links=True, style=True, javascript=True)
    clean_html = cleaner.clean_html(html)
    return clean_html

def process(record):

    def clean_html(html):
        cleaner = Cleaner(page_structure=False, links=True, style=True, javascript=True)
        clean_html = cleaner.clean_html(html)
        return clean_html

    id = record.rec_headers.get_header("WARC-TREC-ID")
    raw_data = record.content_stream().read()
    result = {"_id": id, "text": "", "html": "", "raw": raw_data}

    # Only clean document when data available
    if raw_data.strip() == "":
        return result

    # Parse as LXML as HTML
    lxml_doc = None
    try:
        lxml_doc = lh.fromstring(raw_data)
    except etree.ParseError as e:
        lxml_doc = etree.fromstring(raw_data)
    except Exception as e:
        print("Error Converting to LXML", id, "Error: ", type(e))
        return result
    # Clean HTML
    cleaned_html = clean_html(lxml_doc)
    html_doc = lh.tostring(cleaned_html)
    result["html"] = html_doc

    #Parse HTML for Text
    soup = BeautifulSoup(html_doc, features='lxml')
    soup.prettify()


    text = ""
    try:
        # get article
        text = extract(soup.prettify())
        text = soup.find('p').getText()
        text = text.replace("|","")
        text = re.sub("(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9-_]+)","", text)
        text = text.replace("\n",".")
        words_remove = ["shared by","UberMedia", "A Tweet by","twitter","Twitter","TWITTER"]
        for word in words_remove:
            text = text.replace(word,"")
    except:
        pass

        if (soup.body is not None):
            soup = soup.body

            VALID_TAGS = ['p']
            # Select only relevant tags:
            for tag in soup.findAll('p'):
                if tag.name not in VALID_TAGS:
                    tag.replaceWith(tag.renderContents())
            text = soup.get_text()
            text = text.replace("|","")
            text = re.sub("(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9-_]+)","", text)
            words_remove = ["shared by","UberMedia", "A Tweet by","twitter","Twitter","TWITTER"]
            for word in words_remove:
                text = text.replace(word,"")



    result["text"] = text
    if result["text"] == None or result["text"] == '':
        result['text'] = ''
    else:
        result["text"] = result["text"] #.split(".")


    return result


def ner(record):
    html,id = record['text'],record['_id']


    labels_to_remove = ['CARDINAL',"DATE","TIME","QUANTITY","NORP","ORDINAL","PERCENT","MONEY","PRODUCT"]

    # we loop through each sentence that Spacy finds and save its entities as well.
    doc = nlp(html)
    dic = {}
    for idx,sent in enumerate(doc.sents):
        if sent.ents:
            if len(sent.ents) > 1:
                for sent_ent in sent.ents:
                    if sent_ent.label_ not in labels_to_remove:
                        vals = {"label": sent_ent.label_, "id": id, "s":sent}
                        sent_ent_tr = sent_ent.text
                        sent_ent_tr = sent_ent_tr.replace("the","")
                        if len(sent_ent_tr) < 16 and len(sent_ent_tr) > 3:
                            dic[sent_ent_tr] = vals
            else:
                dic[""] = ""
    if id:

        return dic
    else:
        pass
