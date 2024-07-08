import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os
import json
import boto3

def spark_seesion():

    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [(1,'aftab','kumhari'),(2,'raj','raipur'),(3,'anand','durg')]

    schema = ['id', 'first_name', 'last_name']

    df = spark.createDataFrame(data, schema)

    df.show()

spark_seesion()