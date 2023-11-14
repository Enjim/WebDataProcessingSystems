import csv
import json
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
from tqdm import tqdm

# An API Error Exception
class APIError(Exception):
    def __init__(self, status):
            self.status = status
    def __str__(self):
            return "APIError: status={}".format(self.status)
        
def goldAnnotations(text):
    """Get gold annotations for each sentence from DBPedia.

    Args:
        text (string): sentence from a given document.

    Raises:
        APIError: If status code is not equal to 200 (Succesful linking).

    Returns:
        list: list of dicitionaries with keys: {'entity', 'QID}.
    """
    # DBPedia Spotlight API
    base_url = "http://api.dbpedia-spotlight.org/en/annotate"
    
    # Parameters: 
        #text : text to be annotated
        #confidence : confidence parameter for linking
    params = {"text": text, "confidence": 0.60}
    
    # Response type
    headers = {'accept': 'application/json'}
    
    # Get request
    res = requests.get(base_url, params=params, headers=headers)
    
    # Raise error if code is other than 200
    if res.status_code != 200:
        raise APIError(res.status_code)
    
    # Get wikidata links and QIDs
    result = []
    sparql = SPARQLWrapper("http://dbpedia.org/sparql") #Initialize SPARQL with DBPedia
    sparql.setReturnFormat(JSON) #Set JSON as return format
    
    try:
        for entity in res.json()["Resources"]:  # For every entity detected
            entity_surface = entity["@surfaceForm"].replace(' ','_') # Replace whitespace with '-' for its surface form
            query = "SELECT distinct ?wikidata_concept WHERE {dbr:" + entity_surface + " owl:sameAs ?wikidata_concept FILTER(regex(str(?wikidata_concept), 'www.wikidata.org' ))} LIMIT 1" #Query for entity wikidata link
            sparql.setQuery(query) # Perfrom query
            result.append({'entity':entity_surface, 'QID': sparql.query().convert()['results']['bindings'][0]['wikidata_concept']['value'].removeprefix('http://www.wikidata.org/entity/')}) #Append results
    except:
        pass # If no entities detected, pass.
    
    return result


def testScoring(parsed_corpus, ner_result_file):
    """Calculate metrics (Precision, Recall, F1) for entity recognition and linking.
    
    Args:
        parsed_corpus (list): each element in the list contains a dictionary with parsed elements from each document.
        ner_result_file (csv file): CSV file in the format "{entity detected}, {article name in Wikipedia}, {Wikidata QID}". \
                                    Whitespace in entity names should be replaced with '_'.
    """
    # 1a) Get gold annotations of the corpus using DBPedia Spotlight
    annotations = [] #Initialize list for gold annotations
    for document in tqdm(parsed_corpus):
        for sent in document['text'].split('.'): #For each sentence in a document
            if sent != '' and len(sent) > 20: #Filter sentences that are empty or too short
                annotations.extend(goldAnnotations(sent)) #Get gold annotations for each sentence


    # 1b) Export to json just in case
    with open('GoldAnnotations', 'w') as fout:
        json.dump(annotations, fout)
    
    # ONLY UNCOMMENT WHEN WANT TO USE GoldAnnotations FILE AND COMMENT 1a AND 1b
    # with open('GoldAnnotations') as f:
    #     annotations = json.load(f)
        
    # 2a) Convert csv for detected entities into list of dict
    with open(ner_result_file, encoding='utf-8') as f:
        testing_dict = [{k: v for k, v in row.items()}
            for row in csv.DictReader(f, skipinitialspace=True)]
        
    # 2b) Export to json just in case
    with open('testing_dict', 'w') as f:
        json.dump(testing_dict, f)

    # 3a) Get QID from detected entities
    qid_test = []
    for entity in testing_dict:
        qid_test.append(entity['qid'].strip())

    # 3b) GET QID from gold annotations
    qid_gold = []
    for entity in annotations:
        qid_gold.append(entity['QID'])
        
    # 4) Confusion matrix
    TP = len(list(set(qid_gold).intersection(set(qid_test))))/len(qid_gold)
    FN = len(list(set(qid_gold) - set(qid_test)))/len(qid_gold)
    FP = len(list(set(qid_test) - set(qid_gold)))/len(qid_gold)
     
    # 5) Calculate metrics
    recall = TP/(TP+FN)
    precision = TP/(TP+FP)
    f1 = 2*((precision*recall)/(precision+recall))
    
    # Print result
    print(f"Precision: {precision}\nRecall: {recall}\nF1: {f1}")