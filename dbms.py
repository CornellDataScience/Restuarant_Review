<<<<<<< HEAD

# coding: utf-8

# In[279]:


import pandas as pd
from pyspark.sql import SparkSession
from datetime import date


# In[281]:


=======
import pandas as pd
from pyspark.sql import SparkSession, functions
from datetime import date

### cleaning
>>>>>>> 6f036b0820b95d0b4bb2e78d91810ccc65a0eb6e
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
<<<<<<< HEAD
new_df = pd.DataFrame(big_list,columns=["key", "website", "restaurant","date", "review", "rating", "num_votes"])
new_df.date = new_df.date.map(lambda x: date(*format_date(x)))


# In[282]:


def rating_map(rest_rating, rating):
    if rating == rest_rating:
        return 1
    return 0

def get_ratings(df): # takes in pandas DataFrame
    rating = df[["restaurant", "rating"]]
    for i in range(1,6):
        rating["rating_" + str(i)] = rating.rating.apply(lambda x: rating_map(i,x))
    rating = rating.groupby("restaurant").sum()
    return rating


# In[283]:


get_ratings(new_df).head()


# In[235]:

=======
new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes"])
new_df.date = new_df.date.map(lambda x: date(*format_date(x)))
####
>>>>>>> 6f036b0820b95d0b4bb2e78d91810ccc65a0eb6e

spark = SparkSession.builder.appName('restaurant_reviews').getOrCreate()
spark_df = spark.createDataFrame(new_df)
# spark_df.write.saveAsTable("yelp")
<<<<<<< HEAD


# In[236]:


a = spark_df.groupBy("restaurant").count()


# In[237]:


a.show()

=======
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
>>>>>>> 6f036b0820b95d0b4bb2e78d91810ccc65a0eb6e
