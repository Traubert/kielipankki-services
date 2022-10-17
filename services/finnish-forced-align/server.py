#!/usr/bin/python

from flask import Flask, request, Response, jsonify
import json
from io import BytesIO
import sys
import logging
import datetime
import redis
import uuid
import threading
import subprocess
import pydub
import requests
import time
import platform
import re
import os
import shutil
from tempfile import TemporaryFile

MAX_CONTENT_LENGTH = 500*2**20

app = Flask("finnish-forced-align")

STAGING_WRITE_BUSY = "STAGING_WRITE_BUSY"
DATA_DIR_EMPTY = "DATA_DIR_EMPTY"

DataInDir = '/opt/kaldi/egs/src_for_wav'
DataOutDir = '/opt/kaldi/egs/kohdistus'
DataInDirStaging = '/home/app/wav_staging'

redis_conn = redis.Redis(host='redis', port=6379)
if 'finnish-forced-align-states' not in redis_conn:
    redis_conn.set('finnish-forced-align-states',
                   json.dumps({platform.node(): {STAGING_WRITE_BUSY: False, DATA_DIR_EMPTY: True}}))
else:
    states = json.loads(str(redis_conn.get('finnish-forced-align-states'), encoding = 'utf-8'))
    if platform.node() not in states:
        states[platform.node()] = {STAGING_WRITE_BUSY: False, DATA_DIR_EMPTY: True}
        redis_conn.set('finnish-forced-align-states', json.dumps(states))

align_lock = threading.Lock()
staging_lock = threading.Lock()

def validate_transcript(transcript):
    return True

def align():
    # staging_lock.acquire() # all file writes done
    # align_lock.acquire() # previous processing done
    # data_dir_lock_acquire() # previous files gone
    prepare_data_dir()
    # staging_lock.release()
    completed_process = subprocess.run(
        ["/opt/kaldi/egs/align/aligning_with_Docker/bin/align_in_singularity.sh",
         "phone-finnish-finnish.csv", "false", "false", "../src_for_wav" "no"],
        cwd = '/opt/kaldi/egs/align', stderr = subprocess.PIPE, stdout = subprocess.PIPE) # to capture args, pass stdout = subprocess.PIPE, stderr = subprocess.PIPE
    # logging.error(str(completed_process.stdout, encoding = 'utf-8'))
    # logging.error(str(completed_process.stderr, encoding = 'utf-8'))
    # align_lock.release()
    submit_results()
    # data_dir_lock_acquire()
    # return (completed_process.stdout, completed_process.stderr)

def prepare_data_dir():
    if os.path.isdir(DataInDir) or os.path.isdir(DataOutDir):
        return False
    if not os.path.isdir(DataInDirStaging):
        return False
    os.rename(DataInDirStaging, DataInDir)
    os.mkdir(DataInDirStaging)
    os.mkdir(DataOutDir)
    return True

def submit_results():
    dirname = os.listdir(DataOutDir)[0]
    id2result = {}
    for filename in os.listdir(os.path.join(DataOutDir, dirname)):
        if '.' not in filename:
            continue
        prefix, suffix = filename.split('.')
        if prefix not in id2result:
            id2result[prefix] = {suffix: open(os.path.join(DataOutDir, dirname, filename)).read()}
        else:
            id2result[prefix][suffix] = open(os.path.join(DataOutDir, dirname, filename)).read()
    for _id in id2result:
        response = json.loads(str(redis_conn.get(_id), encoding = 'utf-8'))
        response['status'] = 'done'
        response['results'] = {}
        response['processing_finished'] = round(time.time(), 3)
        for suffix in id2result[_id]:
            response['results'][suffix] = id2result[_id][suffix]
        redis_conn.set(_id, json.dumps(response))
    shutil.rmtree(DataInDir)
    shutil.rmtree(DataOutDir)

@app.route('/audio/align/fi/submit_file', methods=["POST"])
def route_submit_file():
    if request.content_length >= MAX_CONTENT_LENGTH:
        return jsonify({'error': 'body size exceeded maximum of {} bytes'}.format(MAX_CONTENT_LENGTH))
    if not request.content_type.startswith('multipart/form-data') or 'audio' not in request.files or 'transcript' not in request.files:
        return jsonify({'error': 'expected multipart/form-data with audio and transcript file'})
    audio_file_name = request.files['audio'].filename
    if '.' not in audio_file_name:
        return jsonify({'error': 'could not determine audio file type'})
    extension = audio_file_name[audio_file_name.rindex('.')+1:]
    try:
        audio = pydub.AudioSegment.from_file(request.files['audio'], format=extension)
    except Exception as ex:
        return jsonify({'error': 'could not process audio file'})
    transcript = str(request.files['transcript'].read(), encoding='utf-8')
    if not validate_transcript(transcript):
        return jsonify({'error': 'transcript file appears invalid'})
    _id = str(uuid.uuid4())
    if not os.path.isdir(DataInDirStaging):
        os.mkdir(DataInDirStaging)
    audio.export(os.path.join(DataInDirStaging, _id + '.wav'), format='wav')
    open(os.path.join(DataInDirStaging, _id + '.txt'), 'w').write(transcript)
    redis_conn.set(_id, json.dumps({'status': 'pending', 'task': 'finnish-forced-align', 'processing_started': round(time.time(), 3)}))
    job = threading.Thread(target=align)
    job.start()
    return jsonify({'jobid': _id, 'file': audio_file_name})

@app.route('/audio/align/fi/query_job', methods=["POST"])
def route_query_job():
    _id = request.get_data(as_text = True)
    if _id not in redis_conn:
        return jsonify({'error': 'job id not available'})
    return jsonify(json.loads(str(redis_conn.get(_id), encoding='utf-8')))
