# import sys
from subprocess import Popen, PIPE
from flask import Flask, request, jsonify
import time
import redis
import uuid
import json
import threading
from . import cnn_sentiment
# from sqlitedict import SqliteDict

app = Flask("kielipankki-services")
s24_sentiment = cnn_sentiment.s24

redis_conn = redis.Redis(host='redis', port=6379)

def sanitize_response(response):
    response.pop('type', None)

@app.route('/text/fi/postag', methods=['POST'])
def postag():
    tagger = Popen(["finnish-postag"], encoding = 'utf-8', stdin = PIPE, stdout = PIPE)
    out, err = tagger.communicate(request.get_data(as_text = True))
    sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line.split('\t'))
        if len(this_sentence) > 0:
            sentences.append(this_sentence)
    return jsonify(sentences)
    
@app.route('/text/fi/nertag', methods=['POST'])
def nertag():
    args = request.args
    process_args = ["finnish-nertag"]
    if 'show-analyses' in args and args['show-analyses'].lower() == 'true':
        process_args.append("--show-analyses")
    tagger = Popen(process_args, encoding='utf-8', stdin = PIPE, stdout = PIPE)
    out, err = tagger.communicate(request.get_data(as_text = True))
    sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line.split('\t'))
        if len(this_sentence) > 0:
            sentences.append(this_sentence)
    return jsonify(sentences)

def nertag_and_commit(to_tag, args, _id):
    if _id not in redis_conn:
        return
    redis_entry = json.loads(redis_conn.get(_id))
    process_args = ["finnish-nertag"]
    if 'show-analyses' in args and args['show-analyses'].lower() == 'true':
        process_args.append("--show-analyses")
    tagger = Popen(process_args, encoding='utf-8', stdin = PIPE, stdout = PIPE)
    out, err = tagger.communicate(to_tag)
    sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line.split('\t'))
        if len(this_sentence) > 0:
            sentences.append(this_sentence)
    redis_entry['result'] = sentences
    redis_entry['processing_ended'] = round(time.time(), 3)
    redis_entry['status'] = 'done'
    redis_conn.set(_id, json.dumps(redis_entry))

@app.route('/text/fi/nertag/query_job', methods=['POST'])
def nertag_query():
    _id = request.get_data(as_text = True)
    if _id not in redis_conn:
        return jsonify({'error': 'job id not available'})
    response = json.loads(redis_conn.get(_id))
    if response.get('type') != 'ner':
        return jsonify({'error': 'job id not available'})
    if response.get('status') == 'pending':
        return jsonify({'status': 'pending'})
    sanitize_response(response)
    return jsonify(response)
    
@app.route('/text/fi/nertag/submit', methods=['POST'])
def nertag_submit():
    args = request.args
    _id = str(uuid.uuid4())
    redis_conn.set(_id, json.dumps({'type': 'ner', 'status': 'pending', 'processing_started': round(time.time(), 3)}))
    to_tag = request.get_data(as_text = True)
    job = threading.Thread(target=nertag_and_commit, args=(to_tag, args, _id))
    job.start()
    return jsonify({'jobid': _id})

@app.route('/text/fi/sentiment', methods=['POST'])
def sentiment():
    tokenizer = Popen(["finnish-tokenize"], encoding = 'utf-8',
                      stdin = PIPE, stdout = PIPE)
    out, err = tokenizer.communicate(request.get_data(as_text = True))
    sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line)
        if len(this_sentence) > 0:
            sentences.append(this_sentence)
    sentences.append(sum(sentences, []))
    sentiments = s24_sentiment.list(sentences)
    return jsonify(sentiment=list(zip(sentences, sentiments)))

@app.route('/text/fi/annotate', methods=['POST'])
def annotate():
    sentences = []
    data = request.get_data(as_text = True)
    tagger = Popen(["finnish-postag"], encoding = 'utf-8', stdin = PIPE, stdout = PIPE)
    out, err = tagger.communicate(data)
    postag_sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line.split('\t'))
        if len(this_sentence) > 0:
            postag_sentences.append(this_sentence)

    tagger = Popen(["finnish-nertag"], encoding='utf-8', stdin = PIPE, stdout = PIPE)
    out, err = tagger.communicate(data)
    nertag_sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line.split('\t'))
        if len(this_sentence) > 0:
            nertag_sentences.append(this_sentence)


    tokenizer = Popen(["finnish-tokenize"], encoding = 'utf-8',
                      stdin = PIPE, stdout = PIPE)
    out, err = tokenizer.communicate(data)
    sentiment_sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line)
        if len(this_sentence) > 0:
            sentiment_sentences.append(this_sentence)
    sentiments = s24_sentiment.list(sentiment_sentences)

    for i in range(max(len(postag_sentences), len(nertag_sentences), len(sentiments))):
        sentences.append({'postagged': postag_sentences[i], 'nertagged': nertag_sentences[i], 'sentiment': sentiments[i]})
    return jsonify(sentences)

