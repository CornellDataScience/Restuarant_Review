import pandas as pd
import numpy as np
from pyspark.sql import SparkSession #, functions
# from pyspark.sql.window import Window
# from pyspark.sql.functions import rank, col
# from pyspark.sql import Row
# from pyspark.sql.types import *
from datetime import date
import os
import shutil
import json
from kafka import KafkaConsumer
import time

spark = SparkSession.builder.appName('restaurant_reviews').getOrCreate()


def initialize_dbms():
    return initialize_yelp(), initialize_zomato()

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
        return spark.read.parquet('yelp.parquet').toPandas()
    else:
        big_list = read_data('YelpData.txt')
        new_df = pd.DataFrame(big_list, columns=["key", "api", "restaurant", "date", "review", "rating", "num_votes","restaurant_id"])
        new_df.date = pd.to_datetime(new_df.date.map(lambda x: x.split()[0]))
        return new_df

def initialize_zomato():
    if os.path.exists('zomato.parquet'):
        print('zomato parquet')
        return spark.read.parquet('zomato.parquet').toPandas()
    else:
        big_list = read_data('ZomatoData2.txt')
        # new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes", "restaurant_id"])
        big_list = read_data('ZomatoData.txt')
        # new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes"])

        new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes", "restaurant_id"])

        new_df.date = pd.to_datetime(new_df.date.map(lambda x: x.split()[0]))
        new_df.key = new_df.key.apply(lambda x: str(x))
        return new_df

def save_yelp(pd_yelp):
    path = 'yelp.parquet'
    temp = 'yelptemp.parquet'
    spark_df = spark.createDataFrame(pd_yelp)
    if(os.path.exists(path)):
        spark_df.write.parquet(temp, mode="Overwrite")
        shutil.rmtree(path)
        temp_df = spark.read.parquet(temp)
        temp_df.write.parquet(path)
        shutil.rmtree(temp)
    else:
        spark_df.write.parquet(path)

def save_zomato(pd_zomato):
    path = 'zomato.parquet'
    temp = 'zomatotemp.parquet'
    spark_df = spark.createDataFrame(pd_zomato)
    if(os.path.exists(path)):
        spark_df.write.parquet(temp, mode="Overwrite")
        shutil.rmtree(path)
        temp_df = spark.read.parquet(temp)
        temp_df.write.parquet(path)
        shutil.rmtree(temp)
    else:
        spark_df.write.parquet(path)

'''
Returns pandas DataFrame with columns corresponding to counts of number of 1-star, 2-star...5-start reviews
'''
def rating_counts(pd_df):
    pd_df = pd_df[["restaurant", "restaurant_id", "rating"]]
    for i in range(1,6):
        pd_df["rating_" + str(i)] = np.where(pd_df.rating == i, 1, 0)
    return pd_df.groupby("restaurant").sum()

'''
Returns pandas DataFrame with columns as review text, date, api retrieved from, corresponding to restaurant_ids provided
'''
def get_review_text_date_api(df_yelp, df_zomato,yelp_id,zomato_id):
    yelp = df_yelp[df_yelp.restaurant_id == yelp_id][["review", "date", "api"]]
    zomato = df_zomato[df_zomato.restaurant_id == zomato_id][["review", "date", "api"]]
    return yelp.append(zomato).reset_index()

'''
Returns dictionary of restaurant id to number of reviews for that restaurant
'''
def get_restaurant_counts(pd_df):
    pd_df = pd_df.groupby("restaurant_id").count()[["key"]]
    pd_df.columns = ["count"]
    return pd_df.to_dict()["count"]

'''
Returns dataframe of total number of votes per restaurant
'''
def get_vote_counts(pd_df):
    return pd_df[["restaurant","num_votes"]].groupby("restaurant").count().rename({"num_votes":"count"})

'''
Returns list of all review texts corresponding to restaurant
'''
def get_review_text(pd_df,rest_id):
    return list(pd_df[pd_df.restaurant_id == rest_id]["review"])

'''
Returns dictionary from restaurant id to a list of the most recent 5 review's ids
'''
def get_top_5_review_ids(pd_df):
    a = pd_df.sort_values(["restaurant", "date"], ascending=[True,False]).groupby("restaurant").head(5)[["key", "restaurant_id"]]
    return a.groupby("restaurant_id")["key"].apply(list).to_dict()

'''
Returns DataFrame with new row added -- dictionary key is review id, to a list of info pertaining to review
'''
def add_rows(pd_df, data_dict):
    for key in data_dict:
        new_row = data_dict[key]
        new_row.insert(0,key)
        row_df = pd.DataFrame([new_row],columns=pd_df.columns)
        row_df.fillna(value=pd.np.nan, inplace=True)
        row_df.date = pd.to_datetime(row_df.date.map(lambda x: x.split()[0]))
        pd_df = pd_df.append(row_df, ignore_index=True)
    return pd_df

'''
Returns a list of lists corresponding to given restaurant id, each inner list corresponds to a review where the 
0th element is the date of the review and the 1st element is the review rating
'''
def get_review_rating_date(yelp_pandas, zomato_pandas, yelp_id, zomato_id):
    if zomato_id != None:
        zomato_slice = zomato_pandas[(zomato_pandas.rating.notnull()) & (zomato_pandas.restaurant_id == zomato_id)]
        zomato_info = zomato_slice[["date", "rating"]].values.tolist()

    if yelp_id != None:
        yelp_slice = yelp_pandas[(yelp_pandas.rating.notnull()) & (yelp_pandas.restaurant_id == yelp_id)]
        yelp_info = yelp_slice[["date","rating"]].values.tolist()

    print(zomato_info)
    print(yelp_info)
    if zomato_id != None and yelp_id != None:
        total = zomato_info + yelp_info

    elif zomato_id != None:
        total = yelp_info

    else:
        total = zomato_info
    return [[entry[0].to_pydatetime().date(), entry[1]] for entry in total]

'''
Choose an interval argument from link:
https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
Returns a dataframe corresponding to restaurant_id; index contains the time-invervals, and 'rating' column is 
    avg rating for that time interval.
'''
def avg_rating_binned(pd_df, rest_id, interval):
    pd_df = pd_df[pd_df.restaurant_id == rest_id]
    pd_df.date = pd_df.date.dt.to_period(interval)
    return pd_df.groupby(pd_df.date).mean()[["rating"]]

'''
Returns dictionary where the keys are the yelp restaurant ids and the corresponding value is the restaurant name
'''
def yelp_id_restaurant_dict(yelp_pandas):
    yelp_slice = yelp_pandas[["restaurant","restaurant_id"]].drop_duplicates()
    return json.loads(yelp_slice.set_index("restaurant_id").to_json())["restaurant"]
    
'''
Returns a dictionary of restaurant id to its average rating
'''
def get_res_avg_rating(pd_df):
    return pd_df.groupby("restaurant_id").mean()[["rating"]].to_dict()["rating"]

# yelp_df = initialize_yelp()
# zomato_df = initialize_zomato()
# yelp_df = initialize_yelp()
# zomato_df = initialize_zomato()
# zomato_df.show()
# save_zomato(zomato_df)
# yelp_df = initialize_yelp()
# yelp_df.show()
# save_yelp(yelp_df)

# print(yelp_id_restaurant_dict(yelp_df))


# print(avg_rating_binned(zomato_df, "17419914", 'M'))
# print(len(get_review_rating_date(yelp_df, zomato_df, "ZzA6l46CKDrHp7tQwV30GA", "17419914")))

# yelp_df = initialize_yelp() # pandas DataFrame

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