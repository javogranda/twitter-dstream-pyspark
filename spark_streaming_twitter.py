from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp - Javo - Madrid")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 5 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 8997
dataStream = ssc.socketTextStream("localhost",8997)

#tweet = dataStream.map(lambda tweet: tweet["text"].split())
# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x,
y: x - y, 600, 30)

windowedCountsSorted = windowedWordCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Print the first 10 elements of each RDD generated in this DStream to the console
windowedCountsSorted.pprint()
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
