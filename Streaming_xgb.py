#!/usr/bin/env python
# coding: utf-8

# # Spark Streaming by Pyspark_API and Kafka_API
# ## Streaming+pyspark+kafka

# In[ ]:

from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from sklearn.externals import joblib
from kafka import KafkaProducer
from sqlalchemy import *
from sqlalchemy import create_engine
from sqlalchemy import update
import pickle
import pandas as pd
import numpy as np
import os
import pyhdfs
import pyspark
import mysql.connector
import datetime

def savetohdfs(d):
    for i in d:
        client.append("/user/cloudera/model_deploy/output/utime.csv","{},{}\n".format(str(i[0]),str(i[1])))


def output_kafka(partition):
# Create producer
    producer = KafkaProducer(bootstrap_servers=broker_list)
# Get each (word,count) pair and send it to the topic by iterating the partition (an Iterable object)
    for i in partition:
        message = "The Patient NO. is {}, need {} minutes to detect by MRI".format(str(i[0]),str(i[1]))
        producer.send(topic, value=bytes(message, "utf8"))
    producer.close()


def output_rdd(rdd):
    rdd.foreachPartition(output_kafka)


def rdd_stats(rdd):
    if(rdd.count() == 0):
        return rdd

    input_df = rdd.map(lambda arr: Row(PNO=arr[0],usetime=arr[1])).toDF()

    data_filter = input_df.select(input_df["PNO"],input_df["usetime"])
    return data_filter.rdd


def put_JDBC(p):
    for i in p:
        i.write.option("driver", "com.mysql.jdbc.Driver") \
					 .jdbc("jdbc:mysql://10.120.14.110:3306", "DB102.sparkt",\
					  properties={"user": "root", "password": "Qqqq@123"})

def put_sqlalchemy(p):
    #conn=mysql.connector.connect(database='DB102',host='10.120.14.110',user='root',password = 'Qqqq@123')
    engine = create_engine('mysql+mysqlconnector://root:Qqqq@123@10.120.14.110:3306/DB102')
    for i in p:
        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        d = {'PNO': [i[0]], 'PRETIME': [i[1]],'Time':t }
        df = pd.DataFrame(data=d)
        df.to_sql('result',con=engine,if_exists='append',index=False)
       





if __name__ == "__main__":


    client = pyhdfs.HdfsClient(hosts="10.120.14.120,9000",user_name="cloudera")

    #ser producer for topic "utime"
    topic = "utime"
    broker_list = '10.120.14.120:9092,10.120.14.120:9093'
    
    spark = SparkSession \
        .builder \
        .getOrCreate()
    
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 5)
    #ser consumer kafkastream take from topic  Pdata
    lines = KafkaUtils.createStream(ssc, "10.120.14.120:2182", "Pdata_for_model", {"Pdata": 3})




    load_file = open("/home/cloudera/HA_ML_prdict_project/predict_model/pima_20190911_xgb.pickle", 'rb')
    MRI_Model = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(MRI_Model)

    #p = lines.map(lambda x:x[0])
       
    r0 = lines.map(lambda x:(x[0],x[1]))
    r1 = lines.map(lambda x: (x[0],[float(x[1].split(",")[0]),float(x[1].split(",")[1]),float(x[1].split(",")[2]),float(x[1].split(",")[3]),\
                              float(x[1].split(",")[4]),float(x[1].split(",")[5]),float(x[1].split(",")[6])]))
    r2 = r1.map(lambda x: (x[0],np.array(x[1],dtype=int)))
    r3 = r2.map(lambda x: (x[0],x[1].reshape(1,-1)))

    r4 = r3.map(lambda x: (x[0],int(rfr_bc.value.predict(x[1]))))
     
    r5 = r4.map(lambda x :(x[0],x[1]//60))
    
    
    #result = p.union(t)
    
    

    #for sparkSQL
    #result.transform
    #action:save or export

    r5.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        
    r5.pprint()
    r6 = r5.transform(rdd_stats)
    
    r5.foreachRDD(lambda rdd: rdd.foreachPartition(put_sqlalchemy))
    
    #r5.foreachRDD(output_rdd)
    #r5.foreachRDD(lambda rdd: rdd.foreachPartition(savetohdfs))
    # Start it
    ssc.start()
    ssc.awaitTermination()

