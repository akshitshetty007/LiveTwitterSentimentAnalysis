# Scalable architecture for real-time Twitter sentiment analysis
This project implements a scalable architecture to monitor and visualize sentiment against a twitter hashtag in real-time. It streams live tweets from Twitter against a hashtag, performs sentiment analysis on each tweet, and calculates the rolling mean of sentiments. This sentiment mean is continuously sent to connected browser clients and displayed in a sparkline graph. 

### System design
Diagram below illustrates different components and information flow (from right to left).
<img width="781" alt="architecture" src="https://user-images.githubusercontent.com/49590517/61725281-1f61ae00-ad8d-11e9-9a8e-9eca397da4f7.png">
### Project breakdown
Project has three parts

#### 1. Web server
WebServer is a python [flask](http://flask.pocoo.org/) server. It fetches data from twitter using [Tweepy](http://www.tweepy.org/). Tweets are pushed into [Kafka](https://kafka.apache.org/). A sentiment analyzer picks tweets from kafka, performs sentiment analysis using [NLTK](http://www.nltk.org/_modules/nltk/sentiment/vader.html) and pushes the result back in Kafka. Sentiment is read by [Spark Streaming](https://spark.apache.org/streaming/) server (part 3), it calculates the rolling average and writes data back in Kafka. In the final step, the web server reads the rolling mean from Kafka and sends it to connected clients via [SocketIo](https://socket.io/). A html/JS client displays the live sentiment in a sparkline graph using [google annotation](https://developers.google.com/chart/interactive/docs/gallery/annotationchart) charts. 

Web server runs each independent task in a separate thread.<br>
**Thread 1**: fetches data from twitter<br>
**Thread 2**: performs sentiment analysis on each tweet<br>
**Thread 3**: looks for rolling mean from spark streaming<br>

All these threads can run as an independent service to provide a scalable and fault tolerant system. 

#### 2. Kafka
Kafka acts as a message broker between different modules running within the web server as well as between web server and spark streaming server. It provides a scalable and fault tolerant mechanism of communication between independently running services.  

#### 3. Calculating rolling mean of sentiments

A separate pyspark program reads sentiment from Kafka using spark streaming, calculates the rolling average using spark window operations, and writes the results back to Kafka. 

### How to run
To run the project
1. Download, setup and run Apache Kafka. I use following commands on Ubuntu from bin dir of kafka
 -bin directory I set it up as KAFKA_HOME in bashrc file using following commands
```
export KAFKA_HOME=/path/to/kafka
export PATH=$KAFKA_HOME/bin:$PATH
```
-Now use following commands:

i)Start Zookeeper
```   
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```
ii)Start Broker
```  
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```
2. Install complete [NLTK](http://www.nltk.org/install.html)

3. Create a [twitter app](https://apps.twitter.com/) and set your keys in<br> `live_twitter_sentiment_analysis/tweet_ingestion/config.py`
This will require developer account on twitter(keys here refers to credentials to your developer account)

4. Install python packages
```
pip install -r /live_twitter_sentiment_analysis/webapp/requirements.txt
```
5. Run webserver
```
python3 live_twitter_sentiment_analysis/main.py
```
6. Run the PySpark project seperately once tweets start streaming.
```
python3 live_twitter_sentiment_analysis/rolling_avg/rolling_avg.py
```
7. open the url `localhost:8001/index.html`
### Output
Here is what final output looks like in browser
![output](https://user-images.githubusercontent.com/49590517/61728290-8f266780-ad92-11e9-9952-7c9751fe09f2.png)
