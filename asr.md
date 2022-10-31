## ASR endpoints

The following endpoints are currently hosted under https://kielipankki.rahtiapp.fi. All endpoints respond with json.

- `/audio/asr/fi/submit_file` (POST)

  Submit a form with a `file` key, eg. `curl -F 'file=@audio.mp3' http://kielipankki.rahtiapp.fi/audio/asr/fi/submit_file`. The response is json, containing a `jobid` key.
  
- `/audio/asr/fi/submit` (POST)

  Submit a wav file as the data payload, eg. `curl --data @audio.wav https://kielipankki.rahtiapp.fi/audio/asr/fi/submit`. The response is json, containing a `jobid` key.
  
- `/audio/asr/fi/query_job` (POST)

  Submit a jobid as data. The response is 

## Response format

