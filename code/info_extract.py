from warcio.archiveiterator import ArchiveIterator
import gzip
from tqdm import tqdm
import time 

from final_EL import enwiki_title_to_wikidata_id, candidate_generation, candidate_ranking
from final_RE import PredicateExtraction
from parse_ner import process,ner
from evaluation import testScoring

from concurrent.futures import ThreadPoolExecutor

import warnings
warnings.filterwarnings("ignore")

import os




# organise code
def pipeline(ele):
    for document in tqdm(ele):
        
        elements_in_doc = {}
        
        for element in list(document):
            if element != "":
            
                gen_list = candidate_generation(element)
                
                jaccard_scores = []
                if gen_list:
                    
                    for gen in gen_list:
                        try:
                            jacccard = candidate_ranking(gen,document[element]['s'].text)
                        except:
                            time.sleep(1)
                            jacccard = candidate_ranking(gen,document[element]['s'].text)
                        jaccard_scores.append(jacccard)
                        
                    index = jaccard_scores.index(max(jaccard_scores))
                    if max(jaccard_scores) > 0.25: # constants
                        try:
                            document[element]['top'] = enwiki_title_to_wikidata_id(gen_list[index])
                        except:
                            time.sleep(1)
                            document[element]['top'] = enwiki_title_to_wikidata_id(gen_list[index])

                        elements_in_doc[document[element]['top']] = [gen_list[index],element]
                        with open("entities.txt","a+", encoding="utf-8") as f:
                            f.write(f"ENTITY: {document[element]['id']}\t{elements_in_doc[document[element]['top']][1]}\t https://en.wikipedia.org/wiki/{str(elements_in_doc[document[element]['top']][0]).replace(' ','_')} \n")
                            f.close()
                        print(f"ENTITY: {document[element]['id']}\t{elements_in_doc[document[element]['top']][1]}\t https://en.wikipedia.org/wiki/{str(elements_in_doc[document[element]['top']][0]).replace(' ','_')}")
                        if TESTING:
                            with open("testing.csv","a+", encoding="utf-8") as f:
                                f.write(f"{elements_in_doc[document[element]['top']][1]},{str(elements_in_doc[document[element]['top']][0]).replace(' ','_')},{document[element]['top']} \n")
                                f.close()



        if len(elements_in_doc) > 2:
            for i,ent1 in enumerate(list(elements_in_doc)):
                # print(ent1)
                for j,ent2 in enumerate(list(elements_in_doc)):
                    # if ent1 != ent2 and i < j and j-i<5 and document[elements_in_doc.get(ent1)[1]]['s'] == document[elements_in_doc.get(ent2)[1]]['s']: # constants
                    if ent1 != ent2 and i < j and j-i<5:
                        a = PredicateExtraction(ent1, ent2)
                        b = PredicateExtraction(ent2, ent1)
                        if a:

                            with open("relations.txt","a+", encoding="utf-8") as f:
                                f.write(f"RELATION: {document[element]['id']} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent1][0].replace(' ','_')} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent2][0].replace(' ','_')} \t {a[1]} \t https://www.wikidata.org/wiki/Property:{a[-1]} \n")
                                f.close()
                            print(f"RELATION: {document[element]['id']} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent1][0].replace(' ','_')} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent2][0].replace(' ','_')} \t {a[1]} \t https://www.wikidata.org/wiki/Property:{a[-1]}")

                        if b:

                            with open("relations.txt","a+", encoding="utf-8") as f:
                                f.write(f"RELATION: {document[element]['id']} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent2][0].replace(' ','_')} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent1][0].replace(' ','_')} \t {b[1]} \t https://www.wikidata.org/wiki/Property:{b[-1]} \n")
                                f.close()
                            print(f"RELATION: {document[element]['id']} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent2][0].replace(' ','_')} \t https://en.wikipedia.org/wiki/{elements_in_doc[ent1][0].replace(' ','_')} \t {b[1]} \t https://www.wikidata.org/wiki/Property:{b[-1]}")




def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

if __name__ == '__main__':
    
    import sys



    # INPUT = "data\warcs\sample.warc.gz"
    # TESTING = False

    test = os.environ['test']
    INPUT = os.environ['input'] # directory of INPUT

    if test =="y":
        TESTING = True
        with open("testing.csv","a+", encoding="utf-8") as f:
            f.write("entity_text,entity_wikipedia,qid\n")
            f.close()

    elif test =="n":
        TESTING = False
    else:
        print("Please provide a test=y or test=n in the docker args. docker run -e test=y")
        sys.exit(0)
    print(f"Testing is set to {TESTING}.")
    print(f"Your input is {INPUT}")


    result = []

    ner_result = []
    if TESTING:
        print("*** BEGINNING IN TESTING MODE ***")
    else:
        print("*** BEGINNING (NO TESTING) ***")

    print("*** STARTED PARSING WARC ***")
    with gzip.open(INPUT, 'rb') as stream:
        for record in ArchiveIterator(stream):
            
            if record.rec_type == 'response' :
                if record.http_headers.get_header('Content-type') == 'text/html':
                        text = process(record)
                        if text['text'] == '' or text['text'] == None:
                            continue
                        else:
                            result.append(text)
    # ner
    print("*** STARTED NER ***")
    for element in tqdm(result):
        ner_res = ner(element)
        ner_result.append(ner_res)
    
    if not TESTING:
        del result

    l1,l2,l3,l4,l5 = list(split(ner_result,5))
    
    del ner_result
    print("*** STARTED EXTRACTION ***")
    pipeline(l1)
    pipeline(l5)

    pipeline(l4)
    pipeline(l3)

    pipeline(l2)
    print("*** FINISHED EXTRACTION ***")

    if TESTING:

        print("*** BEGININNG TESTING AND GOLD ANNOTATIONS ***")
        testScoring(result,"testing.csv")
        print("*** FINISHED TESTING ***")

# python -m spacy download en_core_web_lg
#  python  info_extract.py data\warcs\sample.warc.gz   