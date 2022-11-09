# kielipankki.rahtiapp.fi API reference

## Text endpoints

### `/text/fi/postag` (POST)

### `/text/fi/nertag` (POST)

### `/text/fi/sentiment` (POST)

### `/text/fi/annotate` (POST)

## Audio endpoints

### `/audio/asr/fi/submit_file` (POST)

Submit a form with a `file` key, eg. `curl -F 'file=@audio.mp3' http://kielipankki.rahtiapp.fi/audio/asr/fi/submit_file`. The response is a json object containing a `jobid` key, which is used later for polling for results with `/audio/asr/fi/query_job`.
  
Example output:
  
`{"file":"puhetta.mp3","jobid":"357f3518-afaa-45e9-bda7-b52a60b73000"}`
  
Additional fields may appear, the `jobid` field is the important one.
  
### `/audio/asr/fi/submit` (POST)

Submit a wav file as the data payload, eg. `curl --data-binary @audio.wav https://kielipankki.rahtiapp.fi/audio/asr/fi/submit`. The response is a json object containing a `jobid` key, which is used later for polling for results with `/audio/asr/fi/query_job`.
  
Example output:
  
`{"jobid":"337adadb-37ff-4492-9480-d2ffb1126932"}`
  
### `/audio/asr/fi/query_job` (POST)

Submit a jobid as the data payload. The response may be a partial result, a complete result, a pending result, or an error state.
  
#### Error states

If the submitted job id is unknown to the service, meaning that no such job has been submitted, or has been submitted so long ago that the cache has forgoten about it, the service returns

`{"error": "job id not available"}`

This is also the response if the job has multiple segments, and one or more of those segments has an unknown job id.

#### Pending result

In this case the job is known, but there is no result to report yet, partial or otherwise.

{'status': 'pending'}


### `/audio/asr/fi/query_job/tekstiks`

This is a specialised endpoint that conforms to a particular front-end.

Submit a jobid as the data payload. The response may be a partial result, a complete result, a pending result, or an error state.


