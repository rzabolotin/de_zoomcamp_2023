import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep
import sys

import settings



producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open(sys.argv[1])

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:

    # green
    # 0 VendorID,
    # 1 lpep_pickup_datetime,
    # 2 lpep_dropoff_datetime,
    # 3 store_and_fwd_flag,
    # 4 RatecodeID,
    # 5 PULocationID,
    # 6 DOLocationID,
    # 7 passenger_count,
    # 8 trip_distance,
    # ...

    # fhv
    # 3 PULocationID,
    # 4 DOLocationID,


    if "green" in sys.argv[1]:
        if not row[5]:
            continue
        key = {"PULocationId": (row[5])}
        value = {"type": "green", "PULocationId": (row[5]), "DOLocationID": (row[6])}
    else:
        if not row[3]:
            continue
        key = {"PULocationId": (row[3])}
        value = {"type": "fhv", "PULocationId": (row[3]), "DOLocationID": (row[4])}

    print(value)

    producer.send(settings.TOPIC_NAME, value=value, key=key)
    print("producing")
    sleep(1)