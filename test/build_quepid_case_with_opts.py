import pandas as pd
import requests
import json
import ast

# Read the CSV file and extract required columns
#data = pd.read_csv('RA_testing_5.csv', usecols=['Question', 'DocIds'])
data = pd.read_csv('RA_Test_105.csv',sep=';',header = None, skiprows=1, error_bad_lines=False)
print(data)
# Initialize the final JSON structure
final_json = {"queries": []}

# Iterate through each row in the dataframe
for index, row in data.iterrows():
    question = row[0]
    doc_ids = row[1]

    doc_ids_list = eval(doc_ids)

    doc_ids = ','.join(map(str.strip,doc_ids_list))
    # Prepare payload for Opensearch GET request
    payload = {
        "query": {
            "match": {
                "publication_id": doc_ids
            }
        },
        "_source": ["title", "publication_id", "publish_date_int"]
    }

    # Perform Opensearch GET request
    response = requests.get('https://vpc-di-data-mdc-search-omoa4llzwjbn5ev7i32oyahnvu.us-east-1.es.amazonaws.com/research/_search', json=payload)
    retrieved_data = response.json()  # Extract JSON response

    # Prepare JSON structure for the query
    query_json = {
        "query_text": question,
        "options": {
            "docs": []
        }
    }

    # Extract required fields from Opensearch response and add to JSON structure
    for hit in retrieved_data['hits']['hits']:
        doc_info = {
            "publication_id": hit['_source']['publication_id'],
            "publish_date_int": hit['_source']['publish_date_int'],
            "title": hit['_source']['title']
        }
        query_json['options']['docs'].append(doc_info)

    # Append the query JSON to the final structure
    final_json["queries"].append(query_json)

# Convert final JSON to a string or write to a file
final_json_str = json.dumps(final_json, indent=2)
print(final_json_str)  # Display or save the JSON structure