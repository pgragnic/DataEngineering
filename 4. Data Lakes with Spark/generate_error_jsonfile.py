from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.functions import isnan, count, when, desc, udf, sort_array, asc, avg, lit

import json
import findspark
findspark.init()

from glob import iglob
import shutil
import os
import time

print("Start Spark session")
start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("Wrangling Data issues") \
    .getOrCreate()

print("--- It took %s seconds ---" % (time.time() - start_time))

print("Read Json files")
start_time = time.time()
path = 'sparkify_event_data.json'
user_log = spark.read.json(path)
print("--- It took %s seconds ---" % (time.time() - start_time))

print("Write Json files")
start_time = time.time()
pathToWriteIn = "errors_2/"
user_log.write.json(pathToWriteIn, mode="overwrite")
print("--- It took %s seconds ---" % (time.time() - start_time))

print("Concatenate Json files")
start_time = time.time()
destination = open('errors6.json', 'wb')
for filename in iglob(os.path.join(pathToWriteIn, '*.json')):
    shutil.copyfileobj(open(filename, 'rb'), destination)
destination.close()
print("--- It took %s seconds ---" % (time.time() - start_time))