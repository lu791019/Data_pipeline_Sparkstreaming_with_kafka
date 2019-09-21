#!/usr/bin/env python
# coding: utf-8

# # SparkStreaming-pyspark
# 
# ## SPK Streaming + SKlearn Model

# In[ ]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from sklearn.externals import joblib
import pickle
import pandas as pd
import numpy as np
import os

'''
def batch(xs):
    yield list(xs)

def f1(record):
    try:
        temp = int(record.split(",")[10])
    except ValueError:
        return False
    return True
'''


if __name__ == "__main__":

    sc = SparkContext()
    ssc = StreamingContext(sc, 5)



    load_file = open('/home/cloudera/HA_pre_spark_MRI/rfr_0905_df.pkl', 'rb')
    MRI_Model = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(MRI_Model)

    lines = ssc.socketTextStream("localhost", 9999)

    r1 = lines.map(lambda x: (int(x.split(",")[0]),int(x.split(",")[1]),int(x.split(",")[2]),int(x.split(",")[3]),int(x.split(",")[4]),\
                              int(x.split(",")[5]),int(x.split(",")[6]),int(x.split(",")[7]),int(x.split(",")[8]),int(x.split(",")[9]),int(x.split(",")[10])))
    r2 = r1.map(lambda x: np.array(x,dtype=int))
    r3 = r2.map(lambda x:x.reshape(1,-1))

    r5 = r3.map(lambda x:  rfr_bc.value.predict(x))
    
    #儲存或匯出
    r5.pprint()
    #r5.saveAsTextFile()
    
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate

