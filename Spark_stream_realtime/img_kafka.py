from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import KafkaUtils
from sklearn.externals import joblib
import pickle
import pandas as pd
import numpy as np
import os
import cv2

def transfer(img_byte):
    cv2.imdecode(np.frombuffer(img_byte,dtype='utf-8'),-1)




if __name__ == "__main__":
    sc = SparkContext()
    ssc = StreamingContext(sc,10)

    kafka_stream = KafkaUtils.createStream(ssc,"10.120.14.120:2182","imgss",{"imgtest01": 1})
    img_arr = kafka_stream.map(transfer)
    img_arr.pprint()


    ssc.start()
    ssc.awaitTermination()


