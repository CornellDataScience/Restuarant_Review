import pandas as pd
from pyspark.sql import SparkSession, functions
from datetime import date
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from math import sqrt
import requests
from bs4 import BeautifulSoup
analyser = SentimentIntensityAnalyzer()
##List of all Restaurants
resArray = ['Sushi Osaka',
' Pokelava',
'Ling Ling Restaurant',
'DiBella’s Subs',
'Plantation Bar and Grill',
'Taco Bell',
'Temple of Zeus',
'Italian Carry Out',
'Domino’s Pizza',
'Sicilian Delight',
'Texas Roadhouse - Ithaca',
'Monks On The Commons',
'Kelly’s Dockside Cafe',
'Scale House Brew Pub',
'Northeast Pizza & Beer',
'Buffalo Wild Wings',
'Spring Buffet & Grill',
'Panera Bread',
' Denny’s',
'Firehouse Subs',
'Crossroads Bar & Grille',
'Sal’s Pizzeria',
'Louie’s Lunch',
'Coal Yard Cafe',
'Napoli Pizzeria',
'Inn at Taughannock Restaurant',
'Ithaca Bakery',
'Souvlaki House',
'Oishii Bowl',
'Asian Noodle House',
'Cafe Pacific',
'CoreLife Eatery',
'Chipotle Mexican Grill',
'Covered Bridge Mkt',
'Sahara Mediterranean Restaurant',
' Arby’s',
'Joe’s Restaurant',
'Mia Restaurant',
'Kilpatrick’s Publick House',
'Aladdin’s Natural Eatery',
'Lincoln Street Diner',
' McDonald’s',
' Mitsuba',
'Uncle Joe’s',
'The Rhine House',
'Ling Ling Garden',
'Ten Forward Cafe',
'Casablanca Pizzeria',
'Applebee’s Grill + Bar',
'Press Cafe',
'Moe’s Southwest Grill',
' Subway',
'Rogan’s Corner',
'Little Thai House',
'Miyake Japanese Restaurant',
'Friends & Pho',
'Salsa Fiesta',
'Mattin’s Café',
'Taverna Banfi',
'Ctb Appetizers',
'Circus Truck',
'Luna Inspired Street Food',
' Goldie’s',
'Dolce Delight',
'Sunset Grill',
'Plum Tree Restaurant',
'The Sub Shop',
'Danby Gathery',
' Okenshields',
'Amit Bhatia Libe Café',
'Universal Deli Grocery',
'Apollo Restaurant',
'D P Dough',
'Bibim Bap Korean Restaurant',
'Big Al’s Hilltop Quikstop',
'Taste of Thai',
'Easy Wok',
'Atrium Cafe',
'Little Caesars Pizza',
'Franny’s Food Truck',
'Tibetan Momo Bar',
'Waffle Frolic',
'Razorback BBQ',
'Purity Ice Cream',
'Royal Court Restaurant',
'The Ivy Room',
'Old Mexico',
'2nd Landing Cafe',
'Jason’s Grocery & Deli',
'Cup O’ Jo Café',
'Sinfully Delicious Baking Co.',
'Red & White Cafe',
' Trillium',
'Café Jennie',
'Pudgie’s Pizza & Sub Shops',
'Mama Teresa Pizzeria',
'Taste of Thai Express']

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
new_df = pd.DataFrame(big_list,columns=["key", "api", "restaurant","date", "review", "rating", "num_votes", "restaurant_id"])
new_df.date = new_df.date.map(lambda x: date(*format_date(x)))
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
    return df.select(["restaurant", "num_votes"]).groupBy("restaurant").count().withColumnRenamed("count", "num_votes")

def get_review_text(df, r):
    section = df.where(df.restaurant == r).select(['review']).collect()
    return [cell.review for cell in section]

def get_num_votes(df, r):
    section = df.where(df.restaurant == r).select(["num_votes"]).collect()
    return [cell.num_votes for cell in section]

def get_rating(df, r):
    section = df.where(df.restaurant == r).select(["rating"]).collect()
    return [cell.rating for cell in section]

"""Returns synonyms of 'term' in list order"""
def synonyms(term):
    response = requests.get('http://www.thesaurus.com/browse/{}'.format(term))
    soup = BeautifulSoup(response.text, features = 'lxml')
    section = soup.find('section', {'class': 'synonyms-container'})
    return [span.text for span in section.findAll('span')]

"""
Returns positivity to negativity ratio given restaurant name "restaurantString"
"""
def scorer(restaurantString):
    sum = 0
    neg = 0
    pos = 0
    compound = 0
    counter = 0
    votes = 0
    rating = 0
    for y in get_num_votes(spark_df, restaurantString):
        votes = votes + y
        if votes == 0:
            votes = 1
    for x, y, a in zip(get_review_text(spark_df, restaurantString), get_num_votes(spark_df, restaurantString), get_rating(spark_df, restaurantString)):
        sum = sum + 1
        rating = rating + a
        score = analyser.polarity_scores(x)
        for z in score.values():
            if (counter == 0):
                neg = neg + sqrt((100*y)/votes)*z
                counter = 1
            elif (counter == 1):
                counter = 2
            elif (counter == 2):
                pos = pos + sqrt((100*y)/votes)*z
                counter = 3
            else:
                compound = compound + sqrt((100*y)/votes)*z
                counter = 0
    if neg!=0 or neg!=0.0 or neg!=0.00 or neg!=0.000:
        return round(pos/neg, 3)
    else:
        return pos
    #print("The average rating for " +  restaurantString + " is " + str(round(rating/sum, 3)))

"""
Returns positivity to negativity ratio given restaurant name "restaurantString" and
aspect to analyze "aspect"
"""
def specificScorer(restaurantString, aspect):
    synonym = synonyms(aspect)
    reviews = get_review_text(spark_df, restaurantString)
    sentencesList = []
    b=[]

    for r in reviews:
        sentencesList.append(r.split('. '))
    for z in sentencesList:
        for sentence in z:
            for word in synonym:
                if word in sentence:
                    b.append(sentence)
                    z.remove(sentence)
                    break

    counter = 0
    neg = 0
    pos = 0
    length = len(b)
    if (length == 0):
        return "There are no reviews corresponding to the " + aspect + " of " + restaurantString
    else:
        for x in b:
            score = analyser.polarity_scores(x)
            for z in score.values():
                if (counter == 0):
                    neg = neg + z
                    counter = 1
                elif (counter == 1):
                    counter = 2
                elif (counter == 2):
                    pos = pos + z
                    counter = 3
                else:
                    counter = 0
        if neg!=0 or neg!=0.0 or neg!=0.00 or neg!=0.000:
            return round(pos/neg, 3)
        else:
            return pos

"""Returns sentiment analysis scores of all restaurants in Ithaca in dictionary form"""
def totalScores():
    scoreDict = {}
    for rest in resArray:
        scoreDict[rest] = scorer(rest)
    return scoreDict

"""Returns sentiment analysis scores of a specific aspect of all restaurants in Ithaca in dictionary form"""
def totalSpecificScore(aspect):
    scoreDict = {}
    for rest in resArray:
        scoreDict[rest] = specificScorer(rest, aspect)
    return scoreDict
