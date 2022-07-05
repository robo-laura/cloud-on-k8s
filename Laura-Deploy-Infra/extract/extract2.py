#!/usr/bin/env python3
#-*- coding: utf-8 -*-

# import the Elasticsearch client library
from elasticsearch import Elasticsearch, exceptions

# import JSON and time
import json, time, pandas
import numpy as np


# create a timestamp using the time() method
start_time = time.time()

# declare globals for the Elasticsearch client host
# DOMAIN = "localhost"
# PORT = 9200

# # concatenate a string for the client's host paramater
# host = str(DOMAIN) + ":" + str(PORT)

# declare an instance of the Elasticsearch library
client = Elasticsearch(['https://elastic:lBR6zuVg8P26c15vdg4Z@kibana.infra.laura.systems:443'])

# try:
#     # use the JSON library's dump() method for indentation
#     info = json.dumps(client.info(), indent=4)

#     # pass client object to info() method
#     print ("Elasticsearch client info():", info)

# except exceptions.ConnectionError as err:

#     # print ConnectionError for Elasticsearch
#     print ("\nElasticsearch info() ERROR:", err)
#     print ("\nThe client host:", host, "is invalid or cluster is not running")

#     # change the client's value to 'None' if ConnectionError
#     client = None

# valid client instance for Elasticsearch
if client != None:

    # get all of the indices on the Elasticsearch cluster
    all_indices = ['rasa-prd-all']

    # keep track of the number of the documents returned
    doc_count = 0

    docs = pandas.DataFrame()

    # iterate over the list of Elasticsearch indices
    for num, index in enumerate(all_indices):

        # declare a filter query dict object
        match_all = {
            "size": 1000,
            "query": {
               "bool": {
                "must": [],
                "filter": [
                  {
                  "range": {
                       "@timestamp": {
              "format": "strict_date_optional_time",
              "gte": "2022-06-08T02:40:52.138Z",
              "lte": "2022-06-08T02:51:01.125Z"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }
            }

        # make a search() request to get all docs in the index
        resp = client.search(
            index = index,
            body = match_all,
            scroll = '60s' # length of time to keep search context
        )

        # keep track of pass scroll _id
        old_scroll_id = resp['_scroll_id']

        # use a 'while' iterator to loop over document 'hits'
        while len(resp['hits']['hits']):

            # make a request using the Scroll API
            resp = client.scroll(
                scroll_id = old_scroll_id,
                scroll = '60s' # length of time to keep search context
            )

            # check if there's a new scroll ID
            if old_scroll_id != resp['_scroll_id']:
                print ("NEW SCROLL ID:", resp['_scroll_id'])

            # keep track of pass scroll _id
            old_scroll_id = resp['_scroll_id']

            # print the response results
            print ("\nresponse for index:", index)
            print ("_scroll_id:", resp['_scroll_id'])
            print ('response["hits"]["total"]["value"]:', resp["hits"]["total"]["value"])

            # iterate over the document hits for each 'scroll'
            for doc in resp['hits']['hits']:
                # print ("\n", doc['_id'], doc['_source'])
                # elastic_docs[doc['_id']] = doc
                # doc_count += 1
                # print ("DOC COUNT:", doc_count)
                source_data = doc["_source"]
                _id = doc["_id"]
                doc_data = pandas.Series(source_data, name = _id)
                docs = docs.append(doc_data)
                print (len(docs))

    # print the total time and document count at the end
    print ("\nTOTAL DOC COUNT:", doc_count)


#  create an empty Pandas DataFrame object for docs
# docs = pandas.DataFrame()

# iterate each Elasticsearch doc in list
# print ("\ncreating objects from Elasticsearch data.")
# for num, doc in enumerate(elastic_docs):

#     # get _source data dict from document
#     source_data = doc["_source"]

#     # get _id from document
#     _id = doc["_id"]

#     # create a Series object from doc dict object
#     doc_data = pandas.Series(source_data, name = _id)

#     # append the Series object to the DataFrame object
#     docs = docs.append(doc_data)


"""
EXPORT THE ELASTICSEARCH DOCUMENTS PUT INTO
PANDAS OBJECTS
"""
print ("\nexporting Pandas objects to different file types.")

# export the Elasticsearch documents as a JSON file
docs.to_json("object.json")

# have Pandas return a JSON string of the documents
json_export = docs.to_json() # return JSON data
# print ("\nJSON data:", json_export)

# export Elasticsearch documents to a CSV file
docs.to_csv("object.csv", ",") # CSV delimited by commas

# export Elasticsearch documents to CSV
csv_export = docs.to_csv(sep=",") # CSV delimited by commas
# print ("\nCSV data:", csv_export)

# create IO HTML string
# import io
# html_str = io.StringIO()

# # export as HTML
# # docs.to_html(
# #     buf=html_str,
# #     classes='table table-striped'
# # )

# # print out the HTML table
# print (html_str.getvalue())

# save the Elasticsearch documents as an HTML table
docs.to_html("objectrocket.html")

# print ("\n\ntime elapsed:", time.time()-start_time)

# print the elapsed time
print ("TOTAL TIME:", time.time() - start_time, "seconds.")