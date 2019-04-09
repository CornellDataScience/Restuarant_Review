import pandas as pd
from pyspark.sql import SparkSession, functions
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import date
import os
import shutil
import json
from kafka import KafkaConsumer
from json import loads
import time

spark = SparkSession.builder.appName('restaurant_reviews').getOrCreate()


def format_yelp_date(date):
    temp = date.split("/")
    temp.insert(0, temp[2])
    temp = list(map(int,temp[:3]))
    return temp

def format_zomato_date(date):
    date = date.split()[0]
    temp = date.split("-")
    return list(map(int,temp))

# def initialize_dbms():
#     if os.path.exists('dbms.parquet'):
#         print('using parquet dbms')
#         spark_df = spark.read.parquet("dbms.parquet")
#         spark_df.show()
#         return spark_df
#     else:
#         big_list = []
#         with open('YelpData.txt', 'rb') as f:
#             data = f.readlines()
#         for partial_data in data:
#             df = pd.read_json(partial_data)
#             for column in df.columns:
#                 temp = list(df[column])
#                 temp.insert(0, column)
#                 big_list.append(temp)
#         new_df = pd.DataFrame(big_list, columns=["key", "api", "restaurant", "date", "review", "rating", "num_votes","restaurant_id"])
#         new_df.date = new_df.date.map(lambda x: date(*format_yelp_date(x)))

#         spark_df = spark.createDataFrame(new_df)
#         return spark_df

def initialize_dbms():
    initialize_yelp()
    initialize_zomato()
def read_data(path):
    big_list = []
    with open(path,'rb') as f:
        data = f.readlines()
    for partial_data in data:
        df = pd.read_json(partial_data)
        for column in df.columns:
            temp = list(df[column])
            temp.insert(0, column)
            big_list.append(temp)
    return big_list

def initialize_yelp():
    if os.path.exists('yelp.parquet'):
        print('yelp parquet')
        return spark.read.parquet('yelp.parquet')
    else:
        big_list = read_data('YelpData.txt')
        new_df = pd.DataFrame(big_list, columns=["key", "api", "restaurant", "date", "review", "rating", "num_votes","restaurant_id"])
        new_df.date = new_df.date.map(lambda x: date(*format_yelp_date(x)))
        return spark.createDataFrame(new_df)

def initialize_zomato():
    if os.path.exists('zomato.parquet'):
        print('zomato parquet')
        return spark.read.parquet('zomato.parquet')
    else:
        big_list = read_data('ZomatoData.txt')
        # new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes", "restaurant_id"])
        new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes", "restaurant_id"])

        new_df.date = new_df.date.map(lambda x: date(*format_zomato_date(x)))
        new_df.key = new_df.key.apply(lambda x: str(x))
        schema = StructType([
            StructField("key", StringType(), True),
            StructField("api", StringType(), True),
            StructField("restaurant", StringType(), True),
            StructField("date", DateType(), True),
            StructField("review", StringType(), True),
            StructField("rating", FloatType(), True),
            StructField("num_votes", IntegerType(), True),
            StructField("restaurant__id", StringType(),True)
        ])
        return spark.createDataFrame(new_df, schema=schema)

def save_dbms(spark_df, isYelp):
    path = 'zomato.parquet'
    temp = 'zomatotemp.parquet'
    if isYelp:
        path = 'yelp.parquet'
        temp = 'yelptemp.parquet'
    if(os.path.exists(path)):
        spark_df.write.parquet(temp, mode="Overwrite")
        shutil.rmtree(path)
        temp_df = spark.read.parquet(temp)
        temp_df.write.parquet(path)
        shutil.rmtree(temp)
    else:
        spark_df.write.parquet(path)


# def save_dbms(spark_df):
#     if(os.path.exists('dbms.parquet')):
#         spark_df.write.parquet("dbmstemp.parquet", mode="Overwrite")
#         shutil.rmtree('dbms.parquet')
#         temp_df = spark.read.parquet("dbmstemp.parquet")
#         temp_df.write.parquet("dbms.parquet")
#         shutil.rmtree('dbmstemp.parquet')
#     else :
#         spark_df.write.parquet("dbms.parquet")

def rating_counts(df):
    df = df.select(["restaurant", "rating"])
    for i in range(1,6):
        df = df.withColumn("rating_" + str(i), functions.when(functions.col("rating") == i,1).otherwise(0))
    return df.groupBy("restaurant").sum().toPandas()

def get_review_text_date_api(df_yelp, df_zomato, rest_name):
    yelp = df_yelp.where(df_yelp.restaurant == rest_name).select(["review", "date", "api"])
    zomato = df_zomato.where(df_zomato.restaurant == rest_name).select(["review", "date", "api"])
    return yelp.union(zomato)

def get_restaurant_counts(df):
    df = df.groupBy("restaurant_id").count().toPandas()
    df = df.set_index("restaurant_id")
    return df.to_dict()["count"]

def get_vote_counts(df):
    return df.select(["restaurant", "num_votes"]).groupBy("restaurant").count().withColumnRenamed("count", "num_votes").toPandas()

def get_review_text(df, r):
    section = df.where(df.restaurant == r).select(["review"]).collect()
    return [cell.review for cell in section]

def get_top_5_review_ids(df):
    key_dict = {}
    window = Window.partitionBy(df['restaurant']).orderBy(df['date'].desc())
    top5 = df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)
    keys = top5.groupby("restaurant").agg(functions.collect_list("key").alias("keys"))
    rows = keys.rdd.collect()
    for row in rows:
        key_dict[row.restaurant] = row.keys
    return key_dict

def add_rows(spark_df, data_dict):
    for key in data_dict:
        new_row = data_dict[key]
        new_row.insert(0,key)
        row_df = pd.DataFrame([new_row],columns=spark_df.columns)
        row_df.fillna(value=pd.np.nan, inplace=True)
        if new_row[1].lower().strip() == "zomato":
            row_df.date = row_df.date.map(lambda x: date(*format_zomato_date(x)))
        else:
            row_df.date = row_df.date.map(lambda x: date(*format_yelp_date(x)))
            row_df.key = row_df.key.apply(lambda x: str(x))
        row_spark = spark.createDataFrame(row_df)
        spark_df = spark_df.union(row_spark)
    return spark_df   

<<<<<<< HEAD
# zomato_df = initialize_zomato()
# zomato_df.show()
=======
# def get_review_rating_date(yelp_spark, zomato_spark, yelp_id, zomato_name):
def get_review_rating_date(yelp_spark, zomato_spark, yelp_id, zomato_id):
    zomato_pandas = zomato_spark.toPandas()
    zomato_slice = zomato_pandas[(zomato_pandas.rating.notnull()) & (zomato_pandas.restaurant_id == zomato_id)]
#     zomato_slice = zomato_pandas[(zomato_pandas.rating.notnull()) & (zomato_pandas.restaurant == zomato_name)]
    zomato_info = zomato_slice[["date", "rating"]].values.tolist()
    
    yelp_pandas = yelp_spark.toPandas()
    yelp_slice = yelp_pandas[(yelp_pandas.rating.notnull()) & (yelp_pandas.restaurant_id == yelp_id)]
    yelp_info = yelp_slice[["date","rating"]].values.tolist()
    return zomato_info + yelp_info


'''
Choose an interval argument from link:
https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
Returns a dataframe corresponding to restaurant_id; index contains the time-invervals, and 'rating' column is 
    avg rating for that time interval.
'''
def avg_rating_binned(spark_df, rest_id, interval):
    df = spark_df.toPandas()
    df = df[df.restaurant_id == rest_id]
    df.date = df.date.dt.to_period(interval)
    return df.groupby(df.date).mean()[["rating"]]

def yelp_id_restaurant_dict(yelp_spark):
    yelp_pandas = yelp_spark.toPandas()
    # print(yelp_pandas)
    yelp_slice = yelp_pandas[["restaurant","restaurant_id"]].drop_duplicates()
    return json.loads(yelp_slice.set_index("restaurant_id").to_json())["restaurant"]


yelp_df = initialize_yelp()
print(yelp_id_restaurant_dict(yelp_df))

>>>>>>> 4bbc89082148e3a96442b99f66ae68b8489321c2
# save_dbms(zomato_df, True)
# spark_df = initialize_dbms()

# save_dbms(spark_df)
# save_dbms(spark_df)
# consumer = KafkaConsumer(
#     'numtest',
#      bootstrap_servers=['localhost:9092'],
#      auto_offset_reset='earliest',
#      enable_auto_commit=True,
#      group_id='my-group',
#      value_deserializer=lambda x: loads(x.decode('utf-8')))
#
# for message in consumer:
#     message = message.value
