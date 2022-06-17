#!/usr/bin/python

from flask import Flask, request, jsonify, Response
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
import copy
import pydub
import pydub.silence
import requests
from tempfile import TemporaryFile

app = Flask("kaldi-serve")

redis_conn = redis.Redis(host='redis', port=6379)

# chain model contains all const components to be shared across multiple threads
model = ChainModel(parse_model_specs("model-spec.toml")[0])
params = toml.load("model-spec.toml")

# initialize a decoder that references the chain model
decoder = Decoder(model)
decoder_lock = threading.Lock()

submit_url = 'http://nginx:1337/audio/asr/fi/submit'

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

def decode(data):
    with start_decoding(decoder):
        decoder.decode_wav_audio(data)
        return decoder.get_decoded_results(10, word_level = True, bidi_streaming = False)
    
def decode_and_commit(data, _id, lock):
    retvals = []
    lock.acquire()
    results = decode(data)
    lock.release()
    for result in results:
        retvals.append({"transcript": result.transcript, "confidence": f'{result.confidence:.5f}'})
        # for word in result.words:
        #     retvals[-1]["transcript"] += f'{word.word} {word.start_time} {word.end_time}'
    response = dict(params)
    response['time'] = datetime.datetime.now().strftime("%Y-%m-%d %X")
    response['responses'] = sorted(retvals, key = lambda x: x["confidence"],
                                   reverse = True)
    response['status'] = 'done'
    redis_conn.set(_id, json.dumps(response))

def segmented(audio, _id):
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
    for segment in segments:
        f = TemporaryFile()
        segment.export(f, format='wav')
        f.seek(0)
        audiobytes = f.read()
        response = requests.post(submit_url, data = audiobytes)
        jobs.append({'duration': segment.duration_seconds, 'jobid': json.loads(response.text)["jobid"]})
    redis_conn.set(_id, json.dumps({'segments': jobs}))
    _id = str(uuid.uuid4())
    return jsonify({'jobid': _id})

@app.route('/audio/asr/fi/submit', methods=["POST"])
def route_submit():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'status': 'pending'}))
    job = threading.Thread(target=decode_and_commit, args=(audio_bytes, _id, decoder_lock))
    job.start()
    return jsonify({'jobid': _id})

@app.route('/audio/asr/fi/submit_file', methods=["POST"])
def route_submit_file():
    file_name = request.files['file'].filename
    if not file_name.endswith('.wav'):
        if '.' not in file_name:
            return jsonify({'error': 'could not determine file type'})
        extension = file_name[file_name.rindex('.')+1:]
        try:
            audio = pydub.AudioSegment.from_file(request.files['file'], format=extension)
            f = TemporaryFile()
            audio.export(f, format='wav')
            f.seek(0)
            audio_bytes = f.read()
        except Exception as ex:
            return jsonify({'error': 'could not convert file, ' + str(ex)})
    else:
        audio_bytes = request.files['file'].read()
        if not valid_wav_header(audio_bytes):
            return jsonify({'error': 'invalid wav header'})
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'status': 'pending'}))
    job = threading.Thread(target=decode_and_commit, args=(audio_bytes, _id, decoder_lock))
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
    completed_jobs = {'status': 'done', 'segments': []}
    running_time = 0.0
    for segment in response['segments']:
        segment_id = segment["jobid"]
        if segment_id not in redis_conn:
            return jsonify({'error': f'job id not available'})
        segment_result = json.loads(redis_conn.get(segment_id))
        if segment_result['status'] == 'pending':
            return jsonify({'status': 'pending'})
        duration = float(segment["duration"])
        if 'model' in segment_result:
            if 'model' not in completed_jobs:
                completed_jobs['model'] = segment_result['model']
            del segment_result['model']
        segment_result["start"] = f'{running_time:.2f}'
        running_time += duration
        segment_result["stop"] = f'{running_time:.2f}'
        segment_result["duration"] = f'{duration:.2f}'
        completed_jobs["segments"].append(segment_result)
    return jsonify(completed_jobs)

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
    process_response = json.loads(redis_conn.get(_id))
    if 'segments' not in process_response:
        if process_response["status"] != "done":
            retval["done"] = False
            retval["message"] = "In progress"
            return jsonify(process_response)
        retval["done"] = True
        retval["result"]["sections"].append({"start": running_time, 'speaker': 'S0',
                                             "end": f'{process_response["duration"]:.2f}',
                                             "transcript": process_response["responses"][0]["transcript"]})
        return jsonify(retval)
    retval["result"] = {"speakers": {"S0": {}}, "sections": []}
    running_time = 0.0
    for segment in process_response['segments']:
        segment_id = segment["jobid"]
        if segment_id not in redis_conn:
            retval["error"] = {"code": no_job_error, "message": "one or more job segment id's not found"}
            return jsonify(retval)
        segment_result = json.loads(redis_conn.get(segment_id))
        if segment_result['status'] != 'done':
            retval["done"] = False
            retval["message"] = "In progress"
            return jsonify(retval)
        duration = float(segment["duration"])
        retval["result"]["sections"].append({"start": f'{running_time:.2f}',
                                             "end": f'{running_time+duration:.2f}',
                                             "transcript": segment_result["responses"][0]["transcript"]})
        running_time += duration
    retval["done"] = True
    return jsonify(retval)

@app.route('/audio/asr/fi/segmented', methods=["POST"])
def route_segmented():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})
    f = BytesIO(audio_bytes)
    audio = pydub.AudioSegment.from_file(f, format="wav")
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'status': 'pending'}))
    job = threading.Thread(target=segmented, args=(audio, _id))
    job.start()
    return jsonify({'jobid': _id})

@app.route('/audio/asr/fi', methods=["POST"])
def route_asr():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})

    with start_decoding(decoder):
        decoder.decode_wav_audio(audio_bytes)
        # get the n-best alternatives
        alts = decoder.get_decoded_results(10)

    retvals = []
    for alt in alts:
        retvals.append({"transcript": alt.transcript, "confidence": alt.confidence})
    response = dict(params)
    response['time'] = datetime.datetime.now().strftime("%Y-%m-%d %X")
    response['responses'] = sorted(retvals, key = lambda x: x["confidence"],
                                   reverse = True)
  
    return jsonify(response)
