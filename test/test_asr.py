import requests
import json
import time
import sys
import argparse

parser = argparse.ArgumentParser(description="Test the asr API")
parser.add_argument("--file", default="puhetta.mp3")
parser.add_argument("--local", action="store_true")
parser.add_argument("--query-path", default="")
args = parser.parse_args()

url = "http://kielipankki.rahtiapp.fi/audio/asr/fi"
if args.local:
    url = "http://localhost:1337/audio/asr/fi"
filename = args.file
submit_file_url = url + "/submit_file"
query_url = url + "/query_job" + args.query_path
load_url = "http://kielipankki.rahtiapp.fi/audio/asr/queue"

response = requests.post(
    submit_file_url, files={"file": (filename, open(filename, "rb"))}
)
response_d = json.loads(response.text)
time.sleep(1)
while True:
    query_response = requests.post(query_url, data=response_d["jobid"])
    query_response_d = json.loads(query_response.text)
    if ("status" in query_response_d and query_response_d["status"] == "pending") or (
        "done" in query_response_d and query_response_d["done"] == False
    ):
        time.sleep(1)
        continue
    else:
        duration = (
            query_response_d["processing_finished"]
            - query_response_d["processing_started"]
        )
        print(json.dumps(query_response_d, indent=4))
        print(f"Got result in {duration}")
        break
