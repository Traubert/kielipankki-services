import requests
import json
import time
import sys
import argparse

parser = argparse.ArgumentParser(description = 'Test the texttools API')
parser.add_argument('--local', action='store_true')
parser.add_argument('--query-path', default='')
args = parser.parse_args()

url = 'http://kielipankki.rahtiapp.fi/text/fi'
if args.local:
    url = 'http://localhost:1337/text/fi'
ner_url = url + '/nertag'
postag_url = url + '/postag'
sentiment_url = url + '/sentiment'
annotate_url = url + '/annotate'

instring = '''
Keravan Teboililla kävi kuhina, kun ei voi voita voittaa mikään."
'''

response = requests.post(ner_url + '/submit', data = instring.encode('utf-8'))
response_d = json.loads(response.text)
time.sleep(1)
while True:
    query_response = requests.post(ner_url + '/query_job', data = response_d['jobid'])
    query_response_d = json.loads(query_response.text)
    if ('status' in query_response_d and query_response_d['status'] == 'pending') or ('done' in query_response_d and query_response_d['done'] == False):
        time.sleep(1)
        continue
    else:
        duration = query_response_d['processing_finished'] - query_response_d['processing_started']
        print(json.dumps(query_response_d, indent=4))
        print(f'Got result in {duration}')
        break
