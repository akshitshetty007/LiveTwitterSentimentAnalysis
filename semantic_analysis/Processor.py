"""
Module to do sentiment analysis on tweets
"""
from kafka import KafkaConsumer,KafkaProducer
from kafka.errors import  KafkaError
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime
from json import dumps
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x: dumps(x).encode('utf-8'))


class Processor(object):
    """
    monitors kafka topic for tweets and performs  sentiment analysis
    """
    kafka_consumer=None
    kafka_producer=None

    target_topic=None
    charset=None
    sid=SentimentIntensityAnalyzer()

    def __init__(self,source_topic,target_topic,kafka_connection,encoding):
        """
        to required initialisations
        :param source_topic:kafka topics to fetch tweets from
        :param target_topic:kafka topic to put tweets
        :param kafka_connection:kafka connection string ip:port
        :param encoding:tweet decoder
        """
        self.kafka_producer=KafkaProducer(bootstrap_servers=kafka_connection,value_serializer=lambda x:dumps(x).encode('utf-8'))
        self.kafka_consumer=KafkaConsumer(source_topic,bootstrap_servers=[kafka_connection])
        self.charset=encoding
        self.target_topic=target_topic

    def start(self):
        """
        start the sentiment analysis
        :rtype:object
        :return:
        """
        for msg in self.kafka_consumer:
            print('-----new message for sentiment analysis-----')
            message=msg.value.decode(self.charset)
            print(message)
            sentiment= self.sid.polarity_scores(message)
            sentiment=sentiment['compound']
            sentiment=(sentiment+1)/2
            data = {}
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            group='tweet'

            data['time'] = now
            data['sentiment'] = sentiment
            data['group']=group

            # sentiment=str(now) + "\t"+ str(sentiment) + "\t"+str(group)

            # print sentiment
            # producer = KafkaProducer(bootstrap_servers='localhost:9092')
            # producer.send(topic, message)
            # Asynchronous by default

            future = self.kafka_producer.send(self.target_topic,value=data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            pass

            # Successful result returns assigned partition and offset
            # print 'topic', (record_metadata.topic)
            # print 'partition', (record_metadata.partition)
            # print 'offset', (record_metadata.offset)
            # self.kafka_producer.put(self.target_topic, sentiment['compound'], self.charset)



