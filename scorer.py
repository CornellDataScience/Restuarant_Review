import pandas as pd
from datetime import date
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from math import sqrt
import requests
from bs4 import BeautifulSoup
import dbms
today = date.today()
m = today.month
y = today.year
d = today.day
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


spark_df = dbms.initialize_yelp()
zomato_df = dbms.initialize_zomato()
def get_num_votes(df, r):    
    sublist = df.loc[df['restaurant_id'] == r]
    return sublist[['num_votes']]

def get_rating(df, r):
    sublist = df.loc[df['restaurant_id'] == r]
    return sublist[['rating']]

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
      
    for y in get_num_votes(spark_df, restaurantString)['num_votes']:
        votes = votes + y
    if votes == 0:
        votes = 1
    for x, y, a in zip(dbms.get_review_text(spark_df, restaurantString), get_num_votes(spark_df, restaurantString)['num_votes'], get_rating(spark_df, restaurantString)['rating']):
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
    sentencesList = []
    b=[]
    
        
    sentencesList = restaurantString.split('.')
    for sentence in sentencesList:
        for word in synonym:
            if word in sentence:
                b.append(sentence)
                break
    counter = 0
    neg = 0
    pos = 0
    length = len(b)
    if (length == 0):
        return None
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
            return round(sqrt(pos/neg), 3)
        else:
            return pos

"""Returns sentiment analysis scores of all restaurants in Ithaca in dictionary form"""
def totalScores(yelp_rest_id,zomato_rest_id):
    scoreDict = {}

    textList = dbms.get_review_text(spark_df,yelp_rest_id)
    textList2 = (dbms.get_review_text(zomato_df,zomato_rest_id))
    fullText = ''
    for text in textList:
        fullText = fullText + text
    for text in textList2:
        fullText = fullText + text
    categories = ['food','service','staff','price']
    for category in categories:
        scoreDict[category] = specificScorer(fullText,category)
    return scoreDict
"""Returns sentiment analysis scores of a specific aspect of all restaurants in Ithaca in dictionary form"""
def totalSpecificScore(aspect):
    scoreDict = {}
    for rest in resArray:
        scoreDict[rest] = specificScorer(rest, aspect)
    return scoreDict

def score_over_time(rest_name):
    today = date.today()
    m = today.month
    y = today.year
    a = True
    datesList = [today]
    for x in range(0,10):
        if a:
            datesList.append(date(y-1, m + 6, 1))
            y = y - 1
            a = False
        else:
            datesList.append(date(y, m, 1))
            a = True
    df = spark_df.loc[spark_df['restaurant_id'] == rest_name]
    dates = df[['date']]
    
    reviews = dbms.get_review_text(spark_df, rest_name)
    print(datesList)
    
    separated = []
    for dateIndex in range(0, len(datesList) - 1):
        n = []
        for revIndex in range(0, len(reviews)):
            if (datesList[dateIndex] > dates['date'][revIndex] and datesList[dateIndex + 1] <= dates['date'][revIndex]):
                n.append(reviews[revIndex])
        separated.append(n)
    print(separated)
    s = []
    for r in separated:
        s.append(grade(r))

    return s

def grade(review):
    counter = 0
    neg = 0
    pos = 0
    for r in review:
        score = analyser.polarity_scores(r)
        for sc in score.values():
            if (counter == 0):
                neg = neg + sc
                counter = 1
            elif (counter == 1):
                counter = 2
            elif (counter == 2):
                pos = pos + sc
                counter = 3
            else:
                counter = 0

    if neg!=0 or neg!=0.0 or neg!=0.00 or neg!=0.000:
        return round(sqrt(pos/neg), 3)
    else:
        return round(pos, 3)

def all_score_over_time(res_id,aspect):
    scoreDict = {}
    for rest in resArray:
        scoreDict[rest] = score_over_time(rest)
    return scoreDict
def competitor_score_over_time(res_id,category):
    finalDict = {}
    new_df = spark_df.loc[spark_df['restaurant_id'] == res_id]
    new_df.sort_values(by='date')
    finalDict[res_id] = []
    for review in new_df['review']:
        score = specificScorer(review,category)
        if(score != None):
            finalDict[res_id].append(score)
    print(finalDict)
    
    
    key = '1c215mO_Get9D6APQHikMmIiiwv2uHBBBuX8z5OAjPR0e_sa67ZHtdQdWHEx4KCnS03wmUqVTyqBdA_bWZifd0YuFf8Ft8mXLSILHY8tvfl5gE9qj5VeHayJzRrJXHYx'
    endpoint = 'https://api.yelp.com/v3/businesses/' + res_id
    head = {'Authorization': 'bearer %s' % key}
    
    r = requests.get(url=endpoint, headers=head)
    data = r.json()
    categories_json = data['categories']
    categories = ''
    for c in categories_json:
        categories += c['alias'] + ', '
    location = data['location']['address1'] \
               + ', ' + data['location']['city'] \
               + ', ' + data['location']['state'] + ' ' \
               + data['location']['zip_code']
    # fetch competitors
    endpoint = 'https://api.yelp.com/v3/businesses/search'
    parameters = {'term': 'restaurants',
                  'limit': 50,
                  'radius': 40000,
                  'location': location,
                  'categories': categories
                  }
    competitors = {}
    competitors[res_id] = data['name']
    r = requests.get(url=endpoint, params=parameters, headers=head)
    data = r.json()
    # get the competitors
    for business in data['businesses']:
        if business['id'] != res_id:
            competitors[business['id']] = business['name']
    for competitor in competitors:
        new_df = spark_df.loc[spark_df['restaurant_id'] == competitor]
        new_df.sort_values(by='date')
        scoreList = []
        for review in new_df['review']:
            score = specificScorer(review,category)
            if(score != None):
               scoreList.append(score)
        if(len(scoreList) > 5):
            finalDict[competitor] = scoreList
            if(len(finalDict) >= 6):
                return finalDict
    return finalDict
