
# coding: utf-8

# In[279]:


import pandas as pd
from pyspark.sql import SparkSession
from datetime import date


# In[281]:


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


spark = SparkSession.builder.appName('restaurant_reviews').getOrCreate()
spark_df = spark.createDataFrame(new_df)
# spark_df.write.saveAsTable("yelp")


# In[236]:


a = spark_df.groupBy("restaurant").count()


# In[237]:


a.show()

