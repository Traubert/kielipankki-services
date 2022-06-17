# import sys
import signal
from subprocess import Popen, PIPE
from flask import Flask, request, jsonify
from . import cnn_sentiment
# from sqlitedict import SqliteDict

app = Flask("kielipankki-services")
s24_sentiment = cnn_sentiment.s24
# cache = SqliteDict("cache.db", autocommit = True)


## add signal handler for SIGINT to properly close 
## the DB when interrupting
# def signal_handler(sig, frame):
#     cache.close()
#     sys.exit(0)

# signal.signal(signal.SIGINT, signal_handler)

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
    return jsonify(postagged=sentences)
    
@app.route('/fi/nertag', methods=['POST'])
def nertag():
    tagger = Popen(["finnish-nertag"], encoding='utf-8', stdin = PIPE, stdout = PIPE)
    out, err = tagger.communicate(request.get_data(as_text = True))
    sentences = []
    for sentence in out.split('\n\n'):
        this_sentence = []
        for line in sentence.split('\n'):
            if line.strip() != '':
                this_sentence.append(line.split('\t'))
        if len(this_sentence) > 0:
            sentences.append(this_sentence)
    return jsonify(nertagged=sentences)
    
@app.route('/fi/sentiment', methods=['POST'])
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

@app.route('/fi/annotate', methods=['POST'])
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

