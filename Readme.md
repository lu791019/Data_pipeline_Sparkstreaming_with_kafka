# 關於 Kafka & SparkStreaming 運用 Scikit Learn Pre-Trained Model 去預測結果的實現方法

# [成果驗收] 病患資料能透過Spark Streaming以及Kafka 配合Scikit-Learn Pre-trained Model 得到預測使用儀器時間的結果

# [On-going] 正在嘗試將data streaming (i) 已 JDBC 存至SQL  (ii)以sparksql df的形式存至metastore 以便後續利用

## 以較簡單的Random Forest測試 (Result則改以 XGB 其MAPE <20%)

## 以spark的broadcast來將模型作為變數

### Scikit Leran Pre-Trained Model
         import joblib

         load_file = open('Model Pickle file', 'rb')

          MRI_Model = joblib.load(load_file)

          load_file.close()

          rfr_bc = sc.broadcast(MRI_Model)



## 程式邏輯過程 (初次將spark應用於實務, 整個步驟由spark框架 逐漸改良優化 spark streaming&Kafka)

## A.hdfs_pre＿DF.py

由 Spark 框架配合 Pandas DataFrame概念實現:

(1) API pyhdfs import CSV file

(2)make Pd Data Frame 

(3) DataFrame ETL 

(4) DateFrame transfering to RDD 

(5) Inferenced by pre-trained model

DF本身就是物件,資料越大 Read&ETL會越花費時間 inference轉換成RDD,等於沒有運用spark的優勢 故此方法效率慢


## B.hdfs_pre.py

Sparkcontent&Textfile transfer to RDD at first

整個過程都在RDD的作動下完成故效率更快


## C.SparkStreaming.py

由第二支程式了解到Spark map/reduce的邏輯推演後,開始進入SparkStreaming (Dstreams)的情境中

以 "nc -lk 9999" 先設定輸入端口（port 9999必須不佔用)

以socketTextStream將端口資料帶入 轉換成Dstreams

之後為map/reduce邏輯推演 將模型變數代入 得到結果


## D.PysparkStreaming_Kafka.py

配合Kafka(2 Topics includint Pdata & utime)協作

因為SparkStreaming API "KafkaUtils.createStream" 

資料能即時進入topic後以Dstreams來作動 

透過程式邏輯推演,資料清理
將模型變數代入 得到結果

## Result

## Streaming_xgb.py

以xgboosting建模(相對更準確,MAPE<20%)


    client = pyhdfs.HdfsClient(hosts="IP & Port",user_name="username")

    #ser producer for topic "utime"
    topic = "utime"
    broker_list = 'Kafka broker IP & Port'
    
    spark = SparkSession \
        .builder \
        .getOrCreate()
    
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 3)
    #ser consumer kafkastream take from topic  Pdata
    lines = KafkaUtils.createStream(ssc, "Kafka Topic IP & Port ", "Topic name", {"Topic name": "partition counts"})




    load_file = open("pre-treained model - pima_20190911_xgb.pickle in predict_model Folder", 'rb')
    MRI_Model = joblib.load(load_file)
    load_file.close()
    rfr_bc = sc.broadcast(MRI_Model)

    #p = lines.map(lambda x:x[0])
    data = lines.map(lambda x: x[1])   
    r0 = lines.map(lambda x:(x[0],x[1]))
    r1 = lines.map(lambda x: (x[0],[float(x[1].split(",")[0]),float(x[1].split(",")[1]),float(x[1].split(",")[2]),\
                              float(x[1].split(",")[3]),float(x[1].split(",")[4]),\
                              float(x[1].split(",")[5]),float(x[1].split(",")[6])]))
    r2 = r1.map(lambda x: (x[0],np.array(x[1],dtype=int)))
    r3 = r2.map(lambda x: (x[0],x[1].reshape(1,-1)))

    r4 = r3.map(lambda x: (x[0],int(rfr_bc.value.predict(x[1]))))
     
    result = r4.map(lambda x :(x[0],x[1]//60))
        
    result.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

結果再透過 ROM sqlalchemy 和 API pyhdfs 儲存至DB(SQL＆HDFS)

## def put_sqlalchemy(p):

    #conn=mysql.connector.connect(DBinfo)

    engine = create_engine('mysql+mysqlconnector:DBinfo')

    for i in p:
        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        d = {'PNO': [i[0]], 'PRETIME': [i[1]],'Time':t }
        df = pd.DataFrame(data=d)
        df.to_sql('result',con=engine,if_exists='append',index=False)
                

## def savetohdfs(d):

    for i in d:
        client.append("/user/cloudera/model_deploy/output/utime.csv","{},{}\n".format(str(i[0]),str(i[1])))

'''     
將資料傳回kafka Topic以便日後使用
'''

def output_kafka(partition):
## Create producer
    producer = KafkaProducer(bootstrap_servers=broker_list)
## Get each (word,count) pair and send it to the topic by iterating the partition (an Iterable object)
    for i in partition:
        message = "The Patient NO. is {}, need {} minutes to detect by MRI".format(str(i[0]),str(i[1]))
        producer.send(topic, value=bytes(message, "utf8"))
    producer.close()


## def output_rdd(rdd):
    rdd.foreachPartition(output_kafka)
'''

## Dstreams Action (foreachRDD)配合foreachPartition 使資料傳輸更流暢



