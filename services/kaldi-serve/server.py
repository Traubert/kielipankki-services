#!/usr/bin/python

from flask import Flask, request, Response, jsonify
import json
from io import BytesIO
from kaldiserve import ChainModel, Decoder, parse_model_specs, start_decoding
import sys
import logging
import toml
import datetime
import redis
import uuid
import threading
import pydub
import pydub.silence
import requests
import time
import platform
from tempfile import TemporaryFile

app = Flask("kaldi-serve")

redis_conn = redis.Redis(host='redis', port=6379)
if 'kaldi_load' not in redis_conn:
    redis_conn.set('kaldi_load', json.dumps({platform.node(): {'decoding_queue_size': 0}}))
else:
    kaldi_load = json.loads(redis_conn.get('kaldi_load'))
    if platform.node() not in kaldi_load:
        kaldi_load[platform.node()] = {'decoding_queue_size': 0}
        redis_conn.set('kaldi_load', json.dumps(kaldi_load))
    elif 'decoding_queue_size' not in kaldi_load[platform.node()]:
        kaldi_load[platform.node()]['decoding_queue_size'] = 0
        redis_conn.set('kaldi_load', json.dumps(kaldi_load))
queue_lock = redis.lock.Lock(redis_conn, "decoding queue lock",
                             blocking = True, blocking_timeout = 1.0)

# chain model contains all const components to be shared across multiple threads
model = ChainModel(parse_model_specs("model-spec.toml")[0])
params = toml.load("model-spec.toml")

# initialize a decoder that references the chain model
decoder = Decoder(model)
decoder_lock = threading.Lock()

submit_url = 'http://nginx:1337/audio/asr/fi/submit'

def increment_decoding_queue_size():
    '''This should be called just before calls to decode_and_commit() or decode()'''
    if queue_lock.acquire():
        kaldi_load = json.loads(redis_conn.get('kaldi_load'))
        kaldi_load[platform.node()]['decoding_queue_size'] += 1
        redis_conn.set('kaldi_load', json.dumps(kaldi_load))
        logging.error(f'incremented queue size in {platform.node()} to ' + str(kaldi_load))
        queue_lock.release()
    else:
        logging.error("Couldn't acquire queue lock!")
def decrement_decoding_queue_size():
    if queue_lock.acquire():
        kaldi_load = json.loads(redis_conn.get('kaldi_load'))
        kaldi_load[platform.node()]['decoding_queue_size'] -= 1
        redis_conn.set('kaldi_load', json.dumps(kaldi_load))
        logging.error(f'decremented queue size in {platform.node()} to ' + str(kaldi_load))
        queue_lock.release()
    else:
        logging.error("Couldn't acquire queue lock!")

def glue_morphs_in_transcript(transcript):
    if '+' not in transcript: return transcript
    return transcript.replace('+ ', '').replace(' +', '').replace('+', '')

def valid_wav_header(data):
    if len(data) < 44:
        return False
    if data[:4] != b'RIFF':
        return False
    if data[8:12] != b'WAVE':
        return False
    if data[12:15] != b'fmt':
        return False
    if data[36:40] != b'data':
        return False
    return True

def decode(data, lock):
    lock.acquire()
    with start_decoding(decoder):
        decoder.decode_wav_audio(data)
        res = decoder.get_decoded_results(1, word_level = True, bidi_streaming = False)
    lock.release()
    decrement_decoding_queue_size()
    return res
    
def decode_and_commit(data, _id, lock):
    retvals = []
    results = decode(data, lock)
    for result in results:
        retvals.append({"transcript": glue_morphs_in_transcript(result.transcript),
                        "confidence": result.confidence,
                        "words": [{"word": word.word, "start": '{:.3f}'.format(word.start_time), "end": '{:.3f}'.format(word.end_time)} for word in result.words]})
    response = dict(params)
    if _id in redis_conn:
        response.update(json.loads(redis_conn.get(_id)))
    response['time'] = datetime.datetime.now().strftime("%Y-%m-%d %X")
    response['responses'] = sorted(retvals, key = lambda x: x["confidence"],
                                   reverse = True)
    response['status'] = 'done'
    response['processing_finished'] = time.time()
    redis_conn.set(_id, json.dumps(response))

def segmented(audio, _id):
    '''Split audio, send parts to asr server, commit job id to redis. '''
    min_segment = 5.0
    segments = pydub.silence.split_on_silence(audio,
                                          min_silence_len = 360,
                                          silence_thresh = -36,
                                          keep_silence = True,
                                          seek_step = 1)
    while len(segments) > 1:
        smallest_duration = audio.duration_seconds
        smallest_duration_idx = 0
        for i, segment in enumerate(segments):
            if segment.duration_seconds < smallest_duration:
                smallest_duration = segment.duration_seconds
                smallest_duration_idx = i
        if smallest_duration >= min_segment:
            break
        if smallest_duration_idx == 0:
            segments[0] += segments[1]
            del segments [1]
        elif smallest_duration_idx == len(segments) - 1:
            segments[-2] += segments[-1]
            del segments[-1]
        else:
            if segments[smallest_duration_idx - 1].duration_seconds < segments[smallest_duration_idx + 1].duration_seconds:
                segments[smallest_duration_idx - 1] += segments[smallest_duration_idx]
                del segments[smallest_duration_idx]
            else:
                segments[smallest_duration_idx] += segments[smallest_duration_idx + 1]
                del segments[smallest_duration_idx + 1]
    jobs = []
    for i, segment in enumerate(segments):
        f = TemporaryFile()
        segment.export(f, format='wav')
        f.seek(0)
        audiobytes = f.read()
        response = requests.post(submit_url, data = audiobytes)
        jobs.append({'duration': segment.duration_seconds, 'jobid': json.loads(response.text)["jobid"]})
    redis_conn.set(_id, json.dumps({'segments': jobs, 'processing_started': time.time()}))

@app.route('/audio/asr/fi/submit', methods=["POST"])
def route_submit():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'status': 'pending', 'processing_started': time.time()}))
    increment_decoding_queue_size()
    job = threading.Thread(target=decode_and_commit, args=(audio_bytes, _id, decoder_lock))
    job.start()
    return jsonify({'jobid': _id})

@app.route('/audio/asr/fi/submit_file', methods=["POST"])
def route_submit_file():
    args = request.args
    do_split = True
    if 'nosplit' in args and args['nosplit'].lower() == 'true':
        do_split = False
    file_name = request.files['file'].filename
    if '.' not in file_name:
        return jsonify({'error': 'could not determine file type'})
    extension = file_name[file_name.rindex('.')+1:]
    
    try:
        audio = pydub.AudioSegment.from_file(request.files['file'], format=extension)
    except Exception as ex:
        return jsonify({'error': 'could not process file, ' + str(ex)})
    
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'status': 'pending', 'processing_started': time.time()}))

    if not do_split:
        f = TemporaryFile()
        audio.export(f, format='wav')
        f.seek(0)
        audio_bytes = f.read()
        increment_decoding_queue_size()
        job = threading.Thread(target=decode_and_commit, args=(audio_bytes, _id, decoder_lock))
        job.start()
    else:
        job = threading.Thread(target=segmented, args=(audio, _id))
        job.start()
    return jsonify({'jobid': _id, 'file': file_name})


@app.route('/audio/asr/fi/query_job', methods=["POST"])
def route_query_job():
    _id = request.get_data(as_text = True)
    if _id not in redis_conn:
        return jsonify({'error': f'job id not available'})
    response = json.loads(redis_conn.get(_id))
    if 'segments' not in response:
        return response
    processing_finished = 0.0
    running_time = 0.0
    for i in range(len(response['segments'])):
        segment_id = response['segments'][i]["jobid"]
        if segment_id not in redis_conn:
            return jsonify({'error': f'job id not available'})
        segment_result = json.loads(redis_conn.get(segment_id))
        if segment_result['status'] == 'pending':
            return jsonify({'status': 'pending'})
        duration = float(response['segments'][i]["duration"])
        if 'model' in segment_result:
            if 'model' not in response:
                response['model'] = segment_result['model']
            del segment_result['model']
        segment_result["start"] = f'{running_time:.2f}'
        running_time += duration
        segment_result["stop"] = f'{running_time:.2f}'
        segment_result["duration"] = f'{duration:.2f}'
        if 'processing_finished' in segment_result:
            segment_processing_finished = float(segment_result['processing_finished'])
            if segment_processing_finished  > processing_finished:
                processing_finished = segment_processing_finished
        response["segments"][i].update(segment_result)
    response['processing_finished'] = processing_finished
    response['status'] = 'done'
    return jsonify(response)

@app.route('/audio/asr/fi/query_job/tekstiks', methods=["POST"])
def route_query_job_tekstiks():
    tekstiks_version = "KP 0.1"
    no_job_error = 40
    internal_error = 41
    transcribing_failed_error = 1
    _id = request.get_data(as_text = True)
    retval = {'id': _id, 'metadata': {'version': tekstiks_version}}
    if _id not in redis_conn:
        retval["done"] = True
        retval["error"] = {"code": no_job_error, "message": "job id not found"}
        return jsonify(retval)
    retval.update(json.loads(redis_conn.get(_id)))
    retval["result"] = {"speakers": {"S0": {}}, "sections": []}
    running_time = 0.0
    processing_finished = 0.0
    if 'segments' not in retval:
        if retval["status"] != "done":
            retval["done"] = False
            retval["message"] = "In progress"
            return jsonify(retval)
        retval["done"] = True
        retval["error"] = {"code": no_job_error, "message": "job has incompatible api request"}
        return jsonify(retval)
        # retval["result"]["sections"].append({"start": running_time, 'speaker': 'S0',
        #                                      "end": f'{process_response["duration"]:.2f}',
        #                                      "transcript": process_response["responses"][0]["transcript"],
        #                                      "words": []})
        # if "words" in process_response:
        #     retval["result"]["sections"][0]["words"] = process_response["words"]
        # return jsonify(retval)
    
    for segment in retval['segments']:
        segment_id = segment["jobid"]
        if segment_id not in redis_conn:
            retval["error"] = {"code": no_job_error, "message": "one or more job segment id's not found"}
            return jsonify(retval)
        segment_result = json.loads(redis_conn.get(segment_id))
        if segment_result['status'] != 'done':
            retval["done"] = False
            retval["message"] = "In progress"
            return jsonify(retval)
        if segment_result.get("processing_finished", 0.0) > processing_finished:
            processing_finished = segment_result["processing_finished"]
        duration = float(segment["duration"])
        retval["result"]["sections"].append({"start": f'{running_time:.2f}',
                                             "end": f'{running_time+duration:.2f}',
                                             "transcript": segment_result["responses"][0]["transcript"],
                                             "words": []})
        running_time += duration
        if "words" in segment_result["responses"][0]:
            retval["result"]["sections"][-1]["words"] = segment_result["responses"][0]["words"]
    retval["done"] = True
    retval["processing_finished"] = processing_finished
    del retval['segments']
    return jsonify(retval)

@app.route('/audio/asr/fi/segmented', methods=["POST"])
def route_segmented():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})
    f = BytesIO(audio_bytes)
    audio = pydub.AudioSegment.from_file(f, format="wav")
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'status': 'pending', 'processing_started': time.time()}))
    job = threading.Thread(target=segmented, args=(audio, _id))
    job.start()
    return jsonify({'jobid': _id})

@app.route('/audio/asr/fi', methods=["POST"])
def route_asr():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})

    increment_decoding_queue_size()
    alts = decode(audio_bytes, decoder_lock)
    decrement_decoding_queue_size()
    
    retvals = []
    for alt in alts:
        retvals.append({"transcript": alt.transcript, "confidence": alt.confidence})
    response = dict(params)
    response['time'] = datetime.datetime.now().strftime("%Y-%m-%d %X")
    response['responses'] = sorted(retvals, key = lambda x: x["confidence"],
                                   reverse = True)
  
    return jsonify(response)
