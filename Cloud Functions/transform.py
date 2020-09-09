from google.cloud import storage
from textblob import TextBlob
import os
import uuid
import json
import logging

def generate_dictionary(cols):
    movie_id = cols[0].split(':')[1]
    user_id = cols[1].split(':')[1]
    user_name = cols[2].split(':')[1]
    helpfulness = cols[3].split(':')[1]
    users = helpfulness.split("/")
    helpful = int(users[0])
    total_users = int(users[1])
    helpfulness_score = 0
    try:
        helpfulness_score = (100*helpful)/total_users
    except:
        helpfulness_score = 0
    score = cols[4].split(':')[1]
    time = cols[5].split(':')[1]
    summary = cols[6].split(':')[1]
    text = cols[7].split(':')[1]
    sentiment = analyse_sentiment(text)

    dict = {}
    dict['movie_id'] = movie_id 
    dict['user_id'] = user_id
    dict['user_name'] = user_name
    dict['helpfulness'] = helpfulness_score
    dict['score'] = score
    dict['time'] = time
    dict['summary'] = summary
    dict['text'] = text
    dict['sentiment'] =  sentiment
    return dict

def analyse_sentiment(text):
    text_blob_obj = TextBlob(text)
    polarity = text_blob_obj.polarity
    if(polarity > 0): 
        return 1
    elif polarity < 0:
        return -1
    else:
        return 0

def has_valid_rows(rows):
    return (rows is not None and len(rows) > 0)

def has_valid_cols(cols):
    return (cols is not None and len(cols) == 8)

def convert_to_json(event, context):
    source_bucket = os.environ.get('SOURCE_BUCKET')
    destination_bucket = os.environ.get('DESTINATION_BUCKET')
    uploaded_filename = event['name']

    client = storage.Client()
    bucket = client.get_bucket(source_bucket)
    blob = bucket.get_blob(uploaded_filename)
    data = blob.download_as_string()
    logging.info("Transforming file: {}".format(uploaded_filename))

    data = data.decode('utf-8')
    rows = data.split("\n\n")

    if(has_valid_rows(rows)):
        pass

    output_str = ""
    for row in rows:
        cols = row.split("\n")
        if(not has_valid_cols(cols)):
            continue
        dict = generate_dictionary(cols)
        output_str = output_str + json.dumps(dict) + '\n'

    output_bucket = client.get_bucket(destination_bucket)
    filename = str(uuid.uuid4()) + '.json'
    output_blob = output_bucket.blob(filename)
    output_blob.upload_from_string(output_str)
    logging.info("Transformed file: {}".format(filename))

    #delete original data
    source_bucket = client.get_bucket(source_bucket)
    source_blob = source_bucket.blob(uploaded_filename)
    source_blob.delete()
    logging.info("Deleted file: {}".format(uploaded_filename))

    return "OK"
