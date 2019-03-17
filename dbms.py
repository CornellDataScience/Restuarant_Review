import pandas as pd
from pyspark.sql import SparkSession, functions
from datetime import date
import os
import shutil
from kafka import KafkaConsumer
from json import loads

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


def initialize_dbms():
    if os.path.exists('dbms.parquet'):
        spark_df = spark.read.parquet("dbms.parquet")
        return spark_df
    else:
        big_list = []
        with open('YelpData.txt', 'rb') as f:
            data = f.readlines()
        for partial_data in data:
            df = pd.read_json(partial_data)
            for column in df.columns:
                temp = list(df[column])
                temp.insert(0, column)
                big_list.append(temp)
        new_df = pd.DataFrame(big_list, columns=["key", "api", "restaurant", "date", "review", "rating", "num_votes","restaurant_id"])
        new_df.date = new_df.date.map(lambda x: date(*format_yelp_date(x)))
        # new_df.date = new_df.date.map(lambda x: date(*format_zomato_date(x)))
        new_df.key= new_df.key.apply(lambda x: str(x))
        spark_df = spark.createDataFrame(new_df)
        return spark_df

def save_dbms(spark_df):
    if(os.path.exists('dbms.parquet')):
        shutil.rmtree('dbms.parquet')
    spark_df.write.parquet("dbms.parquet")


# spark_df.write.saveAsTable("yelp")
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
    df = df.groupBy("restaurant").count().toPandas()
    df = df.set_index("restaurant")
    return df.to_dict()["count"]

def get_vote_counts(df):
    return df.select(["restaurant", "num_votes"]).groupBy("restaurant").count().withColumnRenamed("count", "num_votes").toPandas()

def get_review_text(df, r):
    section = df.where(df.restaurant == r).select(["review"]).collect()
    return [cell.review for cell in section]
<<<<<<< HEAD
df = initialize_dbms()
df.show()
save_dbms(df)
=======
def get_top_5_review_ids(df):
    key_dict = {}
    window = Window.partitionBy(df['restaurant']).orderBy(df['date'].desc())
    top5 = df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)
    keys = top5.groupby("restaurant").agg(functions.collect_list("key").alias("keys"))
    rows = keys.rdd.collect()
    for row in rows:
        key_dict[row.restaurant] = row.keys
    return key_dict

print(type(initialize_dbms()))
>>>>>>> d67b7d025eebd345ed3b05002c33c8855bb14882
# spark_df = initialize_dbms()
# spark_df.show()
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
#     print(message)
