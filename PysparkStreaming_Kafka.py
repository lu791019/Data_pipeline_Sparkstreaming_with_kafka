#!/usr/bin/env python
# coding: utf-8

# # Spark Streaming by Pyspark_API and Kafka_API
# ## Streaming+pyspark+kafka

# In[ ]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from sklearn.externals import joblib
from kafka import KafkaProducer
import pickle
import pandas as pd
import numpy as np
import os
import pyhdfs
import pyspark


def savetohdfs(rdd):
    
    for r in rdd:
        for t in r :
            client.append("/user/cloudera/model_deploy/output/utime.csv","This patient need {} seconds to detect from MRI\n".format(str(t)))


def output_partition(partition):
    # Create producer
    producer = KafkaProducer(bootstrap_servers=broker_list)
    # Get each (word,count) pair and send it to the topic by iterating the partition (an Iterable object)
    for p in partition:
        for t in p :
            message = "The AI predict that {} seconds to detect from  MRI".format(str(t))
            producer.send(topic, value=bytes(message, "utf8"))
    producer.close()


def output_rdd(rdd):
    rdd.foreachPartition(output_partition)



if __name__ == "__main__":

    client = pyhdfs.HdfsClient(hosts="10.120.14.120,9000",user_name="cloudera")

    #ser producer for topic "utime"
    topic = "utime"
    broker_list = '10.120.14.120:9092,10.120.14.120:9093'


    sc = SparkContext()
    ssc = StreamingContext(sc, 3)
    #ser consumer kafkastream take from topic  Pdata
    lines = KafkaUtils.createStream(ssc, "10.120.14.120:2182", "Pdata_for_model", {"Pdata": 3})




    load_file = open("/home/cloudera/HA_ML_prdict_project/predict_model/rfr_0910_df.pkl", 'rb')
    MRI_Model = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(MRI_Model)

    r = lines.map(lambda x:x[0])
    r0 = lines.map(lambda x:x[1])
    r1 = r0.map(lambda x: (int(x.split(",")[0]),int(x.split(",")[1]),int(x.split(",")[2]),int(x.split(",")[3]),int(x.split(",")[4]),\
                   int(x.split(",")[5]),int(x.split(",")[6]),int(x.split(",")[7])))
    r2 = r1.map(lambda x: np.array(x,dtype=int))
    r3 = r2.map(lambda x: x.reshape(1,-1))

    r4 = r3.map(lambda x: rfr_bc.value.predict(x))
    
    #action:save or export

    r4.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    
    r4.pprint()
    
    #r4.foreachRDD(output_rdd)
    
    #r4.foreachRDD(lambda rdd: rdd.foreachPartition(savetohdfs))
    # Start it
    ssc.start()
    ssc.awaitTermination()

