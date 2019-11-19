import csv
from collections import deque
import elasticsearch
from elasticsearch import helpers

def readMovies():
    csvfile = open('ml-latest-small/movies.csv', 'r')

    reader = csv.DictReader( csvfile )

    titleLookup = {}

    for movie in reader:
            titleLookup[movie['movieId']] = movie['title']

    return titleLookup

def readtags():
    csvfile = open('ml-latest-small/tags.csv', 'r')

    titleLookup = readMovies()

    reader = csv.DictReader( csvfile )
    for line in reader:
        tags = {}
        tags['user_id'] = int(line['userId'])
        tags['movie_id'] = int(line['movieId'])
        tags['title'] = titleLookup[line['movieId']]
        tags['tag'] = str(line['tag'])
        tags['timestamp'] = int(line['timestamp'])
        yield tags


es = elasticsearch.Elasticsearch()

es.indices.delete(index="tags",ignore=404)
#deque(helpers.parallel_bulk(es,readRatings(),index="ratings",doc_type="rating"), maxlen=0)
deque(helpers.parallel_bulk(es,readtags(),index="tags"), maxlen=0)
es.indices.refresh()
