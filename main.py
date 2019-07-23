"""
This program launches a flask based web server and 3 threads

1)ingests stream of tweets from twitter against particular hashtag and puts them in kafka topic

2)monitors kafka topic for tweets ,calculates sentiments and put it in another kafka topic

*a seperate python program reads sentiments from kafka topic,calculates rolling mean and puts it in another kafka topic

3)monitors kafka topic for sentiment average and invokes given callback

Average sentiments are sent to clients in real time via  socketio
"""
#monkey patching->
#In Python, the term monkey patch refers to dynamic (or run-time) modifications of a class or module.
# In Python, we can actually change the behavior of code at run-time.
import eventlet
eventlet.monkey_patch()

import socketio
from flask import Flask,current_app
import threads

KAFKA_TOPIC_FOR_AVERAGE = 'average'
KAFKA_TOPIC_FOR_SENTIMENT = 'sentiment'
KAFKA_TOPIC_FOR_TWEETS = 'tweet'
KAFKA_CONNECTION_STRING = 'localhost:9092'
TWEET_ENCODING = 'utf-8'
TWITTER_HASHTAG = 'happy'

sio = socketio.Server()
app = Flask(__name__)

@app.route('/<path:path>')
def index(path):
    """
    servers client web requests
    :param path: request path
    :return:requested resource
    send_static_file(filename)
    Function used internally to send static files from the static folder to the browser.
    """
    print('request on server',path)
    return current_app.send_static_file(path)

@sio.on('connect')
def connect(sid,environment):
    """method gets called when a new client

    :param sid:socket id
    :param environment:
    :return:
    """
    print('connected',sid)

@sio.on('disconnect')
def disconnect(sid):
    """
     methods is called when a socket io connection disconnects
    :param sid: sid of disconnected socket
    :return:
    """
    print('client disconnected',sid)

def average_callback(average):
    """
    method is called whenever rolling average sentiment is received
    from java program via kafka.
    :param average: average sentiment
    """
    # send this average to all connected clients
    sio.emit('event', average)


if __name__ =='__main__':
    threads.launch(
        (
            TWITTER_HASHTAG,
            KAFKA_TOPIC_FOR_TWEETS,
            KAFKA_CONNECTION_STRING,
            TWEET_ENCODING
        ),
        threads.tweet_fetcher
    )

    threads.launch(
        (
            KAFKA_TOPIC_FOR_TWEETS,
            KAFKA_TOPIC_FOR_SENTIMENT,
            KAFKA_CONNECTION_STRING,
            TWEET_ENCODING
        ),
        threads.semantic_analyzer
    )
    threads.launch(
        (
            KAFKA_TOPIC_FOR_AVERAGE,
            average_callback,
            KAFKA_CONNECTION_STRING,
            TWEET_ENCODING
        ),
        threads.average_listener
    )

    app=socketio.Middleware(sio,app)
    print('server started on port 8001')
    eventlet.wsgi.server(eventlet.listen(('',8001)),app)


