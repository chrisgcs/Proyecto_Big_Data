from __future__ import print_function

import sys
import json
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer

def printy(a, b):
  listy = b.collect()
  for l in listy:
    print(l)

def ending(tupla):
    #if tupla[1]==u'end':
    #    return (tupla[1], 1)
    #else:
    file = open("time.txt","a")
    now = datetime.now().time() # time object    
    file.write("tiempo final = "+str(now))
    file.close()
    return (tupla[1], 1)

def handler(message):
    records = message.collect()
    for record in records:
        producer.send('spark-out', str(record))
        producer.flush()

producer = KafkaProducer(bootstrap_servers='dist12.inf.santiago.usm.cl:9092')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        sys.exit(-1)

    file = open("time.txt","w")
    now = datetime.now().time() # time object
    file.write("tiempo inicio = " + str(now) + "\n")
    file.close()
    
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)

    dic = {}

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    #lines = kvs.map(lambda x: x[1])
    #line =  kvs.map(lambda x: json.loads(x[1])).flatMap(lambda dict: dict.items()).filter(lambda items: items[0]=="prod").map(lambda tupler: (tupler[1], 1)).reduceByKey(lambda a, b: a+b).foreachRDD(printy)
    line =  kvs.map(lambda x: json.loads(x[1])).flatMap(lambda dict: dict.items()).filter(lambda items: items[0]=="prod").map(lambda tupler: ending(tupler)).reduceByKey(lambda a, b: a+b).foreachRDD(handler)
    #producto = lines.flatMap(lambda line: line.split(",")[0]).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

    # file = open("time.txt","a")
    # now = datetime.now().time() # time object    
    # file.write("tiempo final = "+str(now))
    # file.close()
    
    #producto.pprint()
    
    #for cu in producto:
    #    if cu[0] in dic.keys():
    #        dic[cu[0]] = cu[1]
    #    else:
    #        dic[cu[0]]+=cu[1]
#
    #dic.pprint()

    ssc.start()
    ssc.awaitTermination()