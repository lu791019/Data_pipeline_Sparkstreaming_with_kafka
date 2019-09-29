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

    input_df = rdd.map(lambda line: line.split(","))\
              .map(lambda arr: Row(PNO=arr[0],usetime=arr[1]))
               #.toDF()
    
    #input_df.createOrReplaceTempView('input_df')

    return input_df
#.map(lambda x : Row(x))        

def send_JDBC(p):
    url = "jdbc:mysql://10.120.14.110:3306"
    connection = createNewConnection("jdbc:mysql://10.120.14.110:3306/sparkt?useSSL=false","root","Qqqq@123")
     #conncection = DriverManager.getConnection("jdbc:mysql://10.120.14.110:3306/sparkt?useSSL=false","root","Qqqq@123")
    for i in p:
        connection.send(i)
    connection.close()


def put_JDBC(p):
    url = "jdbc:mysql://10.120.14.110:3306"
    p.write.jdbc(url=url,table='sparkt',mode='append',preperties={'user':'root','password':'Qqqq@123',\
                      'driver':'com.mysql.jdbc.Driver'})

def put_sqlalchemy(p):
    #conn=mysql.connector.connect(database='DB102',host='10.120.14.110',user='root',password = 'Qqqq@123')
    engine = create_engine('mysql+mysqlconnector://root:Qqqq@123@10.120.14.110:3306/DB102')
    for i in p:
        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        d = {'PNO': [i[0]], 'PRETIME': [i[1]],'Time':t }
        df = pd.DataFrame(data=d)
        df.to_sql('result',con=engine,if_exists='append',index=False)
       





if __name__ == "__main__":


    client = pyhdfs.HdfsClient(hosts="140.115.236.155,9000",user_name="cloudera")

    #ser producer for topic "utime"
    topic = "utime"
    broker_list = '140.115.236.155:9092,140.115.236.155:9093'
    
    spark = SparkSession \
        .builder \
        .getOrCreate()
    
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 3)
    #ser consumer kafkastream take from topic  Pdata
    lines = KafkaUtils.createStream(ssc, "140.115.236.155:2182", "Pdata_for_model", {"Pdata": 3})




    load_file = open("/home/cloudera/HA_ML_prdict_project/predict_model/pima_20190911_xgb.pickle", 'rb')
    MRI_Model = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(MRI_Model)

    #p = lines.map(lambda x:x[0])
    data = lines.map(lambda x: x[1])   
    r0 = lines.map(lambda x:(x[0],x[1]))
    r1 = lines.map(lambda x: (x[0],[float(x[1].split(",")[0]),float(x[1].split(",")[1]),float(x[1].split(",")[2]),float(x[1].split(",")[3]),\
                              float(x[1].split(",")[4]),float(x[1].split(",")[5]),float(x[1].split(",")[6])]))
    r2 = r1.map(lambda x: (x[0],np.array(x[1],dtype=int)))
    r3 = r2.map(lambda x: (x[0],x[1].reshape(1,-1)))

    r4 = r3.map(lambda x: (x[0],int(rfr_bc.value.predict(x[1]))))
     
    result = r4.map(lambda x :(x[0],x[1]//60))
    
  
 
    
    result.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    

    #for sparkSQL
    #result.transform
    #action:save or export        
    
    df_stream = data.transform(rdd_stats)

    
    #data.foreachRDD(lambda rdd: rdd.foreachPartition(send_JDBC))

    #df_stream.write.option("driver", "com.mysql.jdbc.Driver") \
                        #.jdbc("jdbc:mysql://10.120.14.110:3306", "DB102.sparkt",\
                         #properties={"user": "root", "password": "Qqqq@123"})
    #df_stream.foreachRDD(put_JDBC)


    result.foreachRDD(lambda rdd: rdd.foreachPartition(put_sqlalchemy))
    result.foreachRDD(output_rdd)
    result.foreachRDD(lambda rdd: rdd.foreachPartition(savetohdfs))
    result.pprint()
    # Start it
    ssc.start()
    ssc.awaitTermination()

