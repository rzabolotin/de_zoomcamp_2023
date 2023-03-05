# Неделя 5
## Темы: Batch processing, Spark

### Для чего нужен Spark
Spark это технология, которая позволяет обрабатывать очень большие объемы данных.  
Осуществляется это за счет разбиения больших задач на маленькие, которые могут быть выполнены параллельно, и потом агрегирования результатов.  

Чем-то пооже на то, как устроен внутри BigQuery, но в отличие от BigQuery, Spark позволяет работать с данными, которые не хранятся в BigQuery.  

### Что такое Spark
Spark это программа, написанная на языке Scala, но есть API на Java, Python, R.  
Есть кластерный режим и локальный режим.  
Кластерный режим - это когда запускается несколько серверов, они имеют общую точку входа, и задачи отправленные на кластер распределяются между ними. Используетсяч для продакшена.    
Локальный режим - это когда все запускается на одном сервере, удобно использовать для разработки и тестирования. 

### Как установить Spark
Чтобы установить Spark, нужно установить java и скачать бинарные файлы самого Spark. Положить их в PATH и можно использовать.  
После установки будет доступны команды spark-shell, pyspark, spark-submit.  
Для запуска jupyter notebook с поддержкой Spark, нужно перед запуском переопределить переменную PYTHONPATH, чтобы она указывала на папку с библиотеками Spark.
```bash
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"
```

Можно возспользоваться пакетом findspark, который упрощает эту задачу.  
```python
import findspark
findspark.init()
```

### Использование pyspark

Для работы с Spark в Python, нужно создать SparkSession.  
В параметре master указывается адрес кластера, если он есть, или local[*], если нужно запустить локально.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('test') \
    .getOrCreate()
```



### Как загрузить данные в Spark

Spark имеет множество различных источников данных, можно загружать данные из файлов, из баз данных, из HDFS, из S3 и т.д. 

Можно читать файлы из нескольких файлов, например, если файлы разбиты по датам, то можно прочитать все файлы за определенный период.  

```python
df = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)
df = spark.read.csv('data/yellow_tripdata_2019-0[1-3].csv', header=True)
df = spark.read.csv('data/yellow_tripdata_2019-0[1-3]*.csv', header=True)
```

Можно добавлять различные библиотеки для расширения возможности загрузки данных (например Google Cloud Storage).
Тогда нужно определенным образом настроить Spark.
Например, для возможности загрузки файлов из Google Storage можно сделать так:
```python
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()


df_green = spark.read.parquet('gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/*/*')
```

Еще пример чтения данных из SQL server:
```python

df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "employee") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .load()
```

### Как работать с данными в Spark
Существует несколько способов работы с данными в Spark.  

#### Использование объектов DataFrame. 
После загрузки данных мы имеем объект DataFrame, который представляет собой таблицу, в которой есть колонки и строки. Очень похоже на pandas DataFrame.
К нему можно применять различные трансформации и потом получить результат.  
Можно показать в косоли (метод show), или сохранить в новое место (метод write).  

```python
df = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)

df.show(5)
df.write.csv('data/yellow_tripdata_2019-01.csv', header=True)
```

#### Возможные трансформации

##### Выбор колонок
```python
df.select('VendorID', 'tpep_pickup_datetime').show(5)
```

##### Фильтрация
```python
df.filter(df['VendorID'] == 1).show(5)
```

##### Группировка
```python
df.groupBy('VendorID').count().show()
```

##### Пример сложной трансформации
```python
df.select(
    df['VendorID'],
    df['tpep_pickup_datetime'],
    df['tpep_dropoff_datetime'],
    (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).alias('trip_duration')
).filter(df['trip_duration'] > '0 days')\
    .groupBy('VendorID')\
    .agg(
        F.mean('trip_duration').alias('mean_trip_duration'),
        F.stddev('trip_duration').alias('std_trip_duration')
    ).show()

```

##### Объединения
```python
df1 = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)
df2 = spark.read.csv('data/yellow_tripdata_2019-02.csv', header=True)
df3 = spark.read.csv('data/yellow_tripdata_2019-03.csv', header=True)
df1.union(df2).union(df3).show(5)
```

##### Соединения
```python
df1 = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)
df2 = spark.read.csv('data/yellow_tripdata_2019-02.csv', header=True)
df1.join(df2, on='VendorID', how='inner').show(5)
```


#### Использование SQL
Можно использовать SQL для работы с данными.  
Для этого нужно зарегистрировать DataFrame как таблицу, и потом использовать SQL запросы.  
```python
df = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)
df.registerTempTable('yellow_tripdata_2019_01')

df2 = spark.sql('''
    SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, tpep_dropoff_datetime - tpep_pickup_datetime as trip_duration
    FROM yellow_tripdata_2019_01
    WHERE trip_duration > '0 days'
    GROUP BY VendorID
    ORDER BY VendorID
''')

df2.show(5)
```

#### Использование UDF
Можно создавать свои функции для работы с данными.  
```python
def get_hour(dt):
    return dt.hour

get_hour_udf = F.udf(get_hour, IntegerType())

df = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)
df.select(
    df['VendorID'],
    df['tpep_pickup_datetime'],
    get_hour_udf(df['tpep_pickup_datetime']).alias('pickup_hour')
).show(5)
```

#### Использование Window функций
Можно использовать оконные функции для работы с данными.  
```python
from pyspark.sql.window import Window

df = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)
window = Window.partitionBy('VendorID').orderBy('tpep_pickup_datetime')

df.select(
    df['VendorID'],
    df['tpep_pickup_datetime'],
    F.lag(df['tpep_pickup_datetime']).over(window).alias('prev_pickup_datetime'),
    F.lead(df['tpep_pickup_datetime']).over(window).alias('next_pickup_datetime')
).show(5)
```

#### Использование RDD
Можно использовать RDD для работы с данными. Это более низкоуровневый способ.  
```python
df = spark.read.csv('data/yellow_tripdata_2019-01.csv', header=True)
rdd = df.rdd
rdd.take(5)

rdd2 = rdd.map(lambda row: (row['VendorID'], row['tpep_pickup_datetime']))
rdd2.take(5)

rdd3 = rdd2.filter(lambda row: row[0] == '1')
rdd3.take(5)
```

mapPartitions - позволяет работать с целыми партициями данных за раз.

```python
def get_hour(row):
    return row['tpep_pickup_datetime'].hour

rdd2 = rdd.mapPartitions(lambda rows: map(get_hour, rows))
```

### Запись данных в приемник
Spark позволяет записывать данные в различные приемники. Как в файлы, так и во многие типы баз данных.  
```python

df.write.csv('data/yellow_tripdata_2019-01.csv', header=True)
df.write.jdbc('jdbc:postgresql://localhost:5432/postgres', 'yellow_tripdata_2019_01', mode='overwrite', properties=properties)
df.write.parquet('data/yellow_tripdata_2019-01.parquet')
df.write.json('data/yellow_tripdata_2019-01.json')
df.write.format('com.databricks.spark.csv').save('data/yellow_tripdata_2019-01.csv')
df.write.format('bigquery').option('table', output).save()
```

### Чтобы использовать Spark не в Jupyter Notebook
Нужно написать python скрипт и запустить его с помощью spark-submit.
При этом нужно использовать параметры командной строки для передачи параметров в скрипт.  
А настройки кластера задаются также параметрами командной строки (т.е. локальный мастер или же определенный кластер).  

Пример скрипта:
```python
#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df_green = spark.read.parquet(input_green)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet(input_yellow)


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]



df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable('trips_data')


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


df_result.coalesce(1) \
    .write.parquet(output, mode='overwrite')
```

Запуск скрипта:
```bash
URL="spark://de-zoomcamp.europe-west1-b.c.de-zoomcamp-nytaxi.internal:7077"

spark-submit \
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021
```

### Запускать скрипты Spark можно через сервис Google Dataproc

- Нужно для начала создать кластер в Google Dataproc, и указать параметры кластера (количество нод, типы нод, и т.д.).  
- Затем поместить свой скрипт в Google Cloud Storage.  
- Затем запустить скрипт на кластере Dataproc.
  - Можно через веб-интерфейс Dataproc, или через gcloud cli.

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    gs://dtc_data_lake_de-zoomcamp-nytaxi/code/06_spark_sql.py \
    -- \
        --input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_de-zoomcamp-nytaxi/report-2020
```

В результате скрипт запустится на кластере Dataproc, и результаты работы скрипта будут сохранены в Google Cloud Storage. Работа будет происходить на созданных виртуальных машинах, указанных при создании кластера.  


### Ссылки
- https://sparkbyexamples.com/pyspark/
- https://spark.apache.org/docs/latest/api/python/index.html
- 

















