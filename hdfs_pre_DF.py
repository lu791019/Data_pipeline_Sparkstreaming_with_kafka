#!/usr/bin/env python
# coding: utf-8

# ## Pyspark deploying sklearn ML Model to predict from HDFS-csv file
# ### Pyspark+DF+pyhdfs-client

# In[ ]:


from pyspark import SparkConf, SparkContext
from sklearn.externals import joblib
import pickle
import pyhdfs
import pyspark
import pandas as pd
import numpy as np
import os
def batch(xs):
        yield list(xs)

if __name__ == "__main__":

    sc = SparkContext()
    client = pyhdfs.HdfsClient(hosts="192.168.179.138,50070",user_name="cloudera")
    response = client.open("hdfs://localhost/user/cloudera/model_deploy/data/data_0905_nohead.csv")
    text = response.read()
    #print(text)
    data_ = bytes.decode(text).split('\r\n')
    list_=[]
    for i in data_:
        if i =='':
            continue
        else:
            list_.append(i)

    c = ['AGE','TURN','bhour','ITEM_n','ORDERDR_n','MODEL_NAME_n','IO_n','DEPT_n','SEX_n','POS_n','MD_NO_n']
    df = pd.DataFrame(columns=c)
    for j in list_:
        x = j.split(',')
        dlist = [np.int(x[0]),np.int(x[1]),np.int(x[2]),np.int(x[3]),np.int(x[4]),np.int(x[5]),\
                 np.int(x[6]),np.int(x[7]),np.int(x[8]),np.int(x[9]),np.int(x[10])]
        #dlist = [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10]]
        #dr = np.asarray(dlist)
        s = pd.Series(dlist,index=['AGE','TURN','bhour','ITEM_n','ORDERDR_n','MODEL_NAME_n','IO_n','DEPT_n','SEX_n','POS_n','MD_NO_n'])
        #s = pd.Series(dlist)
        df = df.append(s,ignore_index=True)

    df['AGE'] = df['AGE'].astype(np.int64)
    df['TURN'] = df['TURN'].astype(np.int64)
    df['bhour'] = df['bhour'].astype(np.int64)
    df['ITEM_n'] = df['ITEM_n'].astype(np.int64)
    df['ORDERDR_n'] = df['ORDERDR_n'].astype(np.int64)
    df['MODEL_NAME_n'] = df['MODEL_NAME_n'].astype(np.int64)
    df['IO_n'] = df['IO_n'].astype(np.int64)
    df['DEPT_n'] = df['DEPT_n'].astype(np.int64)
    df['SEX_n'] = df['SEX_n'].astype(np.int64)
    df['POS_n'] = df['POS_n'].astype(np.int64)
    df['MD_NO_n'] = df['MD_NO_n'].astype(np.int64)    
    df_ar = df.values

    #sc = SparkContext()
    load_file = open('/home/cloudera/HA_pre_spark_MRI/rfr_0905_df.pkl', 'rb')
    MRI_Model = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(MRI_Model)







    n_partitions=11
    rdd = sc.parallelize(df_ar, n_partitions).zipWithIndex()

    result = rdd.mapPartitions(batch)\
                 .map(lambda xs: ([x[0] for x in xs], [x[1] for x in xs]))\
                 .flatMap(lambda x: zip(x[1], rfr_bc.value.predict(x[0])))

    #result.saveAsTextFile("hdfs://localhost/user/cloudera/output")
    #取五個答案
    print(result.take(1))
    #result.collect()

