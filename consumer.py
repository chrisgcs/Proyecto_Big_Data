from kafka import KafkaConsumer
from json import loads
from datetime import datetime

consumer = KafkaConsumer(
    'spark-out',
     bootstrap_servers=['dist12.inf.santiago.usm.cl:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: x) #loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    print('{}'.format(message))
    if message.split(",")[0] == "(u'end'":
        file = open("time.txt","a")
        now = datetime.now().time() # time object
        file.write("tiempo final = " + str(now))
        file.close()
        print("the end is nigh\n")