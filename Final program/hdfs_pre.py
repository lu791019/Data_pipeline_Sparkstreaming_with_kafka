#!/usr/bin/env python
# coding: utf-8

# # SparkContext-TextFile to prdict by SKlearn model from HDFS
# ## sc.textFile+Pyspark

# In[ ]:


from pyspark import SparkConf, SparkContext
from sklearn.externals import joblib
#from sklearn.model_selection import train_test_split
#from sklearn.preprocessing import LabelEncoder,OneHotEncoder
#from sklearn.preprocessing import MinMaxScaler,StandardScaler
#from sklearn.ensemble import RandomForestRegressor
import pickle
import pyhdfs
import pyspark
import pandas as pd
import numpy as np
import os

def batch(xs):
    yield list(xs)

def f1(record):
    try:
        temp = int(record.split(",")[10])
    except ValueError:
        return False
    return True

if __name__ == "__main__":
    
    sc = SparkContext()
    #path:hdfs://localhost/user/cloudera/model_deploy/data/data_0905_nohead.csv
    data = sc.textFile("hdfs://localhost/user/cloudera/model_deploy/data/data_0905_nohead.csv")
    
    #觀察data結構,每做完一步tranformation最好都take一下來看資料結構
    #data.take(10)
    
    mod_data = data.filter(f1)
    
    
    r1 = mod_data.map(lambda x: (int(x.split(",")[0]),int(x.split(",")[1]),int(x.split(",")[2]),int(x.split(",")[3]),int(x.split(",")[4]),int(x.split(",")[5]),\
                                 int(x.split(",")[6]),int(x.split(",")[7]),int(x.split(",")[8]),int(x.split(",")[9]),int(x.split(",")[10])))
    
    
    
    
    
    #可直接用sklearn model predict
    #以下分為用broadcast和不用的方法
    load_file = open('/home/cloudera/HA_pre_spark_MRI/rfr_0905_df.pkl', 'rb')
    rfr = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(rfr)
    
    #rfr -->map(lambda x: rfr.predict(x))
    #rfr_bc -->map(lambda x: rfr.value.predict(x))
    
    '''
    不用broadcast的話直接帶入即可predict
    result = r2.mapPartitions(batch) \
        .map(lambda xs: ([x[0] for x in xs], [x[1] for x in xs])) \
        .flatMap(lambda x: zip(x[1], rfr.predict(x[0])))
   
    
    用broadcast了話要在.value.predict
    transformation 法一:較複雜,最終會得到: (index, utime) 的結構
    r2 = r1.map(lambda x: np.array(x,dtype=int)).zipWithIndex()
    result = r2.mapPartitions(batch) \
        .map(lambda xs: ([x[0] for x in xs], [x[1] for x in xs])) \
        .flatMap(lambda x: zip(x[1], rfr_bc.value.predict(x[0])))
    '''
    #transformation 法一:較複雜,最終會得到: (index, utime) 的結構 due ot zipWithIndex()
    
    #r2 = r1.map(lambda x: np.array(x,dtype=int)).zipWithIndex()
    #result = r2.mapPartitions(batch).map(lambda xs: ([x[0] for x in xs], [x[1] for x in xs])).flatMap(lambda x: zip(x[1], rfr.predict(x[0])))
    
    
    #transformation 法二:較簡單,最終會得到: array([utime]) 的結構
    r2 = r1.map(lambda x: np.array(x,dtype=int))
    r3 = r2.map(lambda x: x.reshape(1,-1))
    result = r3.map(lambda x: rfr.predict(x))
    
    
    #存入
    result.saveAsTextFile("hdfs://localhost/user/cloudera/model_deploy/output")
    #測試 
    #print(result.take(5))

