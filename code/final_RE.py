import spacy
import requests
from bs4 import BeautifulSoup
from SPARQLWrapper import SPARQLWrapper, JSON
from itertools import cycle
import time

# Define the SPARQL endpoint and set the return format to JSON
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setReturnFormat(JSON)



                    
# extract predicates using sparql
def PredicateExtraction(subject, object):
    # Define the SPARQL query that retrieves the predicate between the subject and object
    query = """
    SELECT ?predicate
    WHERE
    {
    wd:%s ?predicate wd:%s .
    }
    """ % (subject, object)

    # Set the query to be executed by the SPARQL endpoint
    sparql.setQuery(query)

    # Execute the query and obtain the results
    try:
        results = sparql.query().convert()
    except:
        time.sleep(2)
        results = sparql.query().convert()
        

    if results["results"]["bindings"]:
        for result in results["results"]["bindings"]:
            # print(result["predicate"]["value"])
            response = requests.get(result["predicate"]["value"])
            soup = BeautifulSoup(response.text, "html.parser")
            text = soup.find('title').get_text()
    
            return subject, text.rstrip(" - Wikidata"),object,result["predicate"]["value"].split("/")[-1]