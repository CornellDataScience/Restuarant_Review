import pandas as pd
from pyspark.sql import SparkSession, functions
from datetime import date

### cleaning
def format_date(date):
    temp = date.split("/")
    temp.insert(0, temp[2])
    temp = list(map(int,temp[:3]))
    return temp

big_list = []
with open('YelpData.txt', 'rb') as f:
    data = f.readlines()
for partial_data in data:
    df = pd.read_json(partial_data)
    for column in df.columns:
        temp = list(df[column])
        temp.insert(0,column)
        big_list.append(temp)
new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes"])
new_df.date = new_df.date.map(lambda x: date(*format_date(x)))
####

spark = SparkSession.builder.appName('restaurant_reviews').getOrCreate()
spark_df = spark.createDataFrame(new_df)
# spark_df.write.saveAsTable("yelp")
def rating_counts(df):
    df = df.select(["restaurant", "rating"])
    for i in range(1,6):
        df = df.withColumn("rating_" + str(i), functions.when(functions.col("rating") == i,1).otherwise(0))
    return df.groupBy("restaurant").sum()

def get_review_text_date_api(df_yelp, df_zomato, rest_name):
    yelp = df_yelp.where(df_yelp.restaurant == rest_name).select(["review", "date", "api"])
    zomato = df_zomato.where(df_zomato.restaurant == rest_name).select(["review", "date", "api"])
    return yelp.union(zomato)

def get_restaurant_counts(df):
    return df.groupBy("restaurant").count()

def get_vote_counts(df):
    return spark_df.select(["restaurant", "num_votes"]).groupBy("restaurant").count().withColumnRenamed("count", "num_votes")

def get_review_text(df, restaurant):
    section = spark_df.where(spark_df.restaurant == "The Rook").select(["review"]).collect()
    return [cell.review for cell in section]