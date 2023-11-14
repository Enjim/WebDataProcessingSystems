import wikipedia
import requests
import time

def candidate_generation(entity): # generate entitieus
    try:
        result = wikipedia.search(entity,results = 3)
    except:
        time.sleep(1)
        result = wikipedia.search(entity,results = 3)
    return result


def enwiki_title_to_wikidata_id(title: str) -> str:
    """
    from wikipedia title to wikidata ID
    """
    try:
        protocol = 'https'
        base_url = 'en.wikipedia.org/w/api.php'
        params = f'action=query&prop=pageprops&format=json&titles={title}'
        url = f'{protocol}://{base_url}?{params}'

        response = requests.get(url)
        json = response.json()
        for pages in json['query']['pages'].values():
            wikidata_id = pages['pageprops']['wikibase_item']
        return wikidata_id
    except:
        return None

def jaccard_similarity(list1, list2):
    """
    jaccard similarity function
    """
    s1 = set(list1)
    s2 = set(list2)
    return float(len(s1.intersection(s2)) / len(s1.union(s2)))
def candidate_ranking(entity,entity_sentence):
    """
    get summary from wikipedia and compare to sentence in text using jaccard similarity.

    this function returns a float in the range [0,1]
    """
    try:
        result = wikipedia.summary(entity)[250:500]
        sco = jaccard_similarity(entity_sentence,result)
        return sco 
    except:
        return 0
