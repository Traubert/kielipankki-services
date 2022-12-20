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
import re
import subprocess
from tempfile import TemporaryFile, NamedTemporaryFile

MAX_CONTENT_LENGTH = 500*2**20

app = Flask("kaldi-serve")

KALDI_LOAD = 'kaldi_load'
ASR_SEGMENTS = 'asr_segments'
ASR = 'asr'

redis_conn = redis.Redis(host='redis', port=6379)
queue_lock = redis.lock.Lock(redis_conn, "decoding queue lock",
                             blocking = True, blocking_timeout = 1.0)
if queue_lock.acquire():
    if KALDI_LOAD not in redis_conn:
        redis_conn.set(KALDI_LOAD, json.dumps({platform.node(): {'decoding_queue_size': 0}}))
    else:
        kaldi_load = json.loads(redis_conn.get(KALDI_LOAD))
        if platform.node() not in kaldi_load:
            kaldi_load[platform.node()] = {'decoding_queue_size': 0}
            redis_conn.set(KALDI_LOAD, json.dumps(kaldi_load))
        elif 'decoding_queue_size' not in kaldi_load[platform.node()]:
            kaldi_load[platform.node()]['decoding_queue_size'] = 0
            redis_conn.set(KALDI_LOAD, json.dumps(kaldi_load))
    queue_lock.release()
else:
    logging.error("Couldn't acquire queue lock!")

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
        try:
            kaldi_load = json.loads(redis_conn.get(KALDI_LOAD))
            kaldi_load[platform.node()]['decoding_queue_size'] += 1
            redis_conn.set(KALDI_LOAD, json.dumps(kaldi_load))
        except KeyError:
            logging.error(f"Queue db was missing platform {platform.node()}")
        queue_lock.release()
    else:
        logging.error("Couldn't acquire queue lock!")
def decrement_decoding_queue_size():
    if queue_lock.acquire():
        try:
            kaldi_load = json.loads(redis_conn.get(KALDI_LOAD))
            kaldi_load[platform.node()]['decoding_queue_size'] -= 1
            redis_conn.set(KALDI_LOAD, json.dumps(kaldi_load))
        except KeyError:
            logging.error(f"Queue db was missing platform {platform.node()}")
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
                        "words": [{"word": word.word, "start": round(word.start_time, 3), "end": round(word.end_time, 3)} for word in result.words]})
    response = dict(params)
    if _id in redis_conn:
        response.update(json.loads(redis_conn.get(_id)))
    response['time'] = datetime.datetime.now().strftime("%Y-%m-%d %X")
    response['responses'] = sorted(retvals, key = lambda x: x["confidence"],
                                   reverse = True)
    response['status'] = 'done'
    response['processing_finished'] = round(time.time(), 3)
    redis_conn.set(_id, json.dumps(response))

def segmented(audio, _id):
    '''Split audio, send parts to asr server, commit job ids to redis. '''
    if _id in redis_conn:
        redis_entry = json.loads(redis_conn.get(_id))
    else:
        redis_entry = {'type': ASR_SEGMENTS, 'processing_started': round(time.time(), 3)}
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
        jobs.append({'type': ASR, 'duration': segment.duration_seconds, 'jobid': json.loads(response.text)["jobid"]})
    redis_entry['segments'] = jobs
    redis_conn.set(_id, json.dumps(redis_entry))

def sanitize_response(response):
    response.pop('type', None)
    
@app.route('/audio/asr/fi/submit', methods=["POST"])
def route_submit():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'type': ASR, 'status': 'pending', 'processing_started': round(time.time(), 3)}))
    increment_decoding_queue_size()
    job = threading.Thread(target=decode_and_commit, args=(audio_bytes, _id, decoder_lock))
    job.start()
    return jsonify({'jobid': _id})

@app.route('/audio/asr/fi/submit_file', methods=["POST"])
def route_submit_file():
    if request.content_length >= MAX_CONTENT_LENGTH:
        return jsonify({'error': f'body size exceeded maximum of {MAX_CONTENT_LENGTH} bytes'})
    args = request.args
    do_split = True
    if 'nosplit' in args and args['nosplit'].lower() == 'true':
        do_split = False
    if request.content_type.startswith('multipart/form-data'):
        file_name = request.files['file'].filename
        if '.' not in file_name:
            return jsonify({'error': 'could not determine file type'})
        extension = file_name[file_name.rindex('.')+1:]
        try:
            audio = pydub.AudioSegment.from_file(request.files['file'], format=extension)
        except Exception as ex:
            return jsonify({'error': 'could not process file'})
    else:
        if request.content_type.startswith('audio/'):
            if request.content_type.startswith('audio/mpeg'):
                extension = 'mp3'
            elif request.content_type.startswith('audio/vorbis') or request.content_type.startswith('audio/ogg'):
                extension = 'ogg'
            elif request.content_type.startswith('audio/wav') or request.content_type.startswith('audio/x-wav'):
                extension = 'wav'
        elif request.content_type.startswith('application/'):
            extension = 'wav'
        else:
            return jsonify({'error': 'expected either HTML form or mimetype audio/mpeg, audio/vorbis, audio/ogg, audio/wav or audio/x-wav'})
        audio_file = BytesIO(request.get_data(as_text = False))
        
        file_name = ''
        if 'Content-Disposition' in request.headers:
            match = re.search(r'filename="([^"]+)"', request.headers['Content-Disposition'])
            if match:
                file_name = match.group(1)
                
        try:
            audio = pydub.AudioSegment.from_file(audio_file, format=extension)
        except Exception as ex:
            return jsonify({'error': 'could not process file'})

    if extension == 'wav':
        if audio.sample_width > 2 or audio.channels > 1:
            downsample_tmp_read_f = NamedTemporaryFile(suffix = '.wav')
            audio.export(downsample_tmp_read_f.name, format='wav')
            downsample_tmp_write_f = NamedTemporaryFile(suffix = '.wav')
            # For some reason passing arguments to ffmpeg through pydub doesn't seem to work, so we do it this way.
            # -y means overwrite the (temporary) output file, -ac 1 means make it mono if it isn't already, and -c:a pcm_s16le means to use the standard 16 bit encoder for the audio codec
            downsampler = subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", f"{downsample_tmp_read_f.name}", "-ac", "1", "-c:a", "pcm_s16le", f"{downsample_tmp_write_f.name}"])
            audio = pydub.AudioSegment.from_file(downsample_tmp_write_f.name, format=extension)
    _id = str(uuid.uuid4())

    if not do_split:
        f = TemporaryFile()
        audio.export(f, format='wav')
        f.seek(0)
        audio_bytes = f.read()
        increment_decoding_queue_size()
        redis_conn.set(_id, json.dumps({'type': ASR, 'status': 'pending', 'processing_started': round(time.time(), 3)}))
        job = threading.Thread(target=decode_and_commit, args=(audio_bytes, _id, decoder_lock))
        job.start()
    else:
        redis_conn.set(_id, json.dumps({'type': ASR_SEGMENTS, 'status': 'pending', 'processing_started': round(time.time(), 3)}))
        job = threading.Thread(target=segmented, args=(audio, _id))
        job.start()
    return jsonify({'jobid': _id, 'file': file_name})


@app.route('/audio/asr/fi/query_job', methods=["POST"])
def route_query_job():
    _id = request.get_data(as_text = True)
    if _id not in redis_conn:
        return jsonify({'error': f'job id not available'})
    response = json.loads(redis_conn.get(_id))
    if response.get('type') == ASR:
        sanitize_response(response)
        return response
    if response.get('type') != ASR_SEGMENTS:
        return jsonify({'error': 'job id not available'})
    if 'segments' not in response:
        return jsonify({'error': 'internal server error'})
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
        segment_result["start"] = round(running_time, 3)
        running_time += duration
        segment_result["stop"] = round(running_time, 3)
        segment_result["duration"] = round(running_time, 3)
        if 'processing_finished' in segment_result:
            segment_processing_finished = float(segment_result['processing_finished'])
            if segment_processing_finished  > processing_finished:
                processing_finished = segment_processing_finished
        response["segments"][i].update(segment_result)
    response['processing_finished'] = processing_finished
    response['status'] = 'done'
    sanitize_response(response)
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
    if retval.get('type') not in (ASR, ASR_SEGMENTS):
        retval["done"] = True
        retval["error"] = {"code": no_job_error, "message": "job id not found"}
        sanitize_response(retval)
        return jsonify(retval)
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
            retval["done"] = True
            return jsonify(retval)
        segment_result = json.loads(redis_conn.get(segment_id))
        if segment_result['status'] != 'done':
            retval["done"] = False
            retval["message"] = "In progress"
            return jsonify(retval)
        if segment_result.get("processing_finished", 0.0) > processing_finished:
            processing_finished = segment_result["processing_finished"]
        duration = float(segment["duration"])
        retval["result"]["sections"].append({"start": round(running_time, 3),
                                             "end": round(running_time+duration, 3),
                                             "transcript": segment_result["responses"][0]["transcript"],
                                             "words": []})
        running_time += duration
        if "words" in segment_result["responses"][0]:
            retval["result"]["sections"][-1]["words"] = segment_result["responses"][0]["words"]
    retval["status"] = "done"
    retval["done"] = True
    retval["processing_finished"] = processing_finished
    del retval['segments']
    sanitize_response(retval)
    return jsonify(retval)

@app.route('/audio/asr/fi/segmented', methods=["POST"])
def route_segmented():
    audio_bytes = bytes(request.get_data(as_text = False))
    if not valid_wav_header(audio_bytes):
        return jsonify({'error': 'invalid wav header'})
    f = BytesIO(audio_bytes)
    audio = pydub.AudioSegment.from_file(f, format="wav")
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'type': ASR_SEGMENTS, 'status': 'pending', 'processing_started': round(time.time(), 3)}))
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

@app.route('/audio/asr/fi/queue', methods=["GET"])
def route_queue():
    kaldi_load = json.loads(redis_conn.get(KALDI_LOAD))
    queue = 0
    for pod in kaldi_load:
        queue += kaldi_load[pod]["decoding_queue_size"]
    return jsonify({"total decoding queue size": queue})
