#!/usr/bin/env python
# coding: utf-8

# # Spark Streaming by Pyspark_API and Kafka_API
# ## Streaming+pyspark+kafka

# In[ ]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from sklearn.externals import joblib
import pickle
import pandas as pd
import numpy as np
import os



if __name__ == "__main__":

    sc = SparkContext()
    ssc = StreamingContext(sc, 3)

    lines = KafkaUtils.createStream(ssc, "192.168.179.138:2182", "consumer", {"HA03": 1})




    load_file = open('/home/cloudera/HA_pre_spark_MRI/rfr_0905_df.pkl', 'rb')
    MRI_Model = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(MRI_Model)

    r0 = lines.map(lambda x: x[1])
    r1 = r0.map(lambda x: (int(x.split(",")[0]),int(x.split(",")[1]),int(x.split(",")[2]),int(x.split(",")[3]),int(x.split(",")[4]),\
                   int(x.split(",")[5]),int(x.split(",")[6]),int(x.split(",")[7]),int(x.split(",")[8]),int(x.split(",")[9]),int(x.split(",")[10])))
    r2 = r1.map(lambda x: np.array(x,dtype=int))
    r3 = r2.map(lambda x:x.reshape(1,-1))

    r4 = r3.map(lambda x:  rfr_bc.value.predict(x))

    #儲存或匯出
    r4.pprint()
    #r4.foreachRDD(lambda rdd: rdd.foreach(lambda x: print(x)))
    # Start it
    ssc.start()
    ssc.awaitTermination()

