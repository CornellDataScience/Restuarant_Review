import json
import requests
import numpy as np
from kafka import KafkaProducer

# registered API key is 4dded5ab75a73b4c37bf996ffd3e1a5b

# ithaca information on Zomato
      # "entity_type": "subzone",
      # "entity_id": 137164,
      # "title": "Ithaca, Finger Lakes Region",
      # "latitude": 42.4433723277,
      # "longitude": -76.4956000244,
      # "city_id": 991,
      # "city_name": "Finger Lakes Region",
      # "country_id": 216,
      # "country_name": "United States"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                      json.dumps(x).encode('utf-8'))
def main():
    # Order of Json String Dictionary: {ReviewID: [ReviewSiteName, RestaurantName, ReviewDate, ReviewText, ReviewRating, ReviewVotes, isElite]}
    url = 'https://developers.zomato.com/api/v2.1/search?entity_id=137164&entity_type=subzone'


    #  &start=20
    response_json = requests.get(url, headers={'user-key': '4dded5ab75a73b4c37bf996ffd3e1a5b'})

    restaurants_num = json.loads(response_json.text)["results_found"]

    restaurants_list = json.loads(response_json.text)["restaurants"]

    # fetch all the restaurants up to 100
    for i in range(20, 100, 20):
        new_url = url + "&start=" + str(i)
        response_json = requests.get(new_url, headers={'user-key': '4dded5ab75a73b4c37bf996ffd3e1a5b'})
        restaurants = json.loads(response_json.text)["restaurants"]
        restaurants_list.extend(restaurants)
    review_list = {}

    # reviews are in format:
    # fetch reviews for restaurants one by one
    # format: {ReviewID: [ReviewSiteName, RestaurantName, ReviewDate, ReviewText, ReviewRating, ReviewVotes, isElite]}
    for restaurant in restaurants_list:
        new_url = "https://developers.zomato.com/api/v2.1/reviews?res_id=" + str(restaurant["restaurant"]["R"]["res_id"])
        response_json = requests.get(new_url, headers={'user-key': '4dded5ab75a73b4c37bf996ffd3e1a5b'})
        user_reviews = json.loads(response_json.text)["user_reviews"]
        for review in user_reviews:
            review_list[review["review"]["id"]] = ["Zomato", restaurant["restaurant"]["name"], review["review"]["review_time_friendly"], review["review"]["review_text"], review["review"]["rating"], review["review"]["likes"], (review["review"]["user"]["foodie_level_num"] > 5)]
    return review_list
    # save review dictionary to json file
    #with open('Zomato_review.json', 'w') as fp:
        #json.dump(review_list, fp)


# scrape the 5 reviews for restaurantID
# the review format is in {ReviewID: [ReviewSiteName, RestaurantName, ReviewDate, ReviewText, ReviewRating, ReviewVotes, isElite]}
def scrape_reviews_from_restaurant_id(restaurantID):
    Review_Results = {}

    url = "https://developers.zomato.com/api/v2.1/"


    reviews_url = url + "reviews?res_id=" + str(restaurantID)
    response_json = requests.get(reviews_url, headers={'user-key': '4dded5ab75a73b4c37bf996ffd3e1a5b'})
    user_reviews = json.loads(response_json.text)["user_reviews"]
    restaurant_ID_list = []
    for review in user_reviews:
        restaurant_ID_list.append(review["review"]["id"])
    return restaurant_ID_list


# scrapes specific number of reviews for restaurantID
# the review format is in {ReviewID: [ReviewSiteName, RestaurantName, ReviewDate, ReviewText, ReviewRating, ReviewVotes, isElite]}
def scrape_latest_reviews(numReviews, restaurantID):
    Review_Results = {}

    url = "https://developers.zomato.com/api/v2.1/"

    # get the restaurant name
    restaurant_url = url + "restaurant?res_id=" + str(restaurantID)
    response_json = requests.get(restaurant_url, headers={'user-key': '4dded5ab75a73b4c37bf996ffd3e1a5b'})
    restaurant_name = json.loads(response_json.text)["name"]

    reviews_url = url + "reviews?res_id=" + str(restaurantID)
    response_json = requests.get(reviews_url, headers={'user-key': '4dded5ab75a73b4c37bf996ffd3e1a5b'})
    user_reviews = json.loads(response_json.text)["user_reviews"]
    for i in range(numReviews):
        if i == len(user_reviews):
            print("CANNOT GET THE LATEST REVIEWS. NOT ENOUGH LATEST REVIEWS.")
        review = user_reviews[i]
        Review_Results[review["review"]["id"]] = ["Zomato", restaurant_name,
                                                  review["review"]["review_time_friendly"],
                                                  review["review"]["review_text"],
                                                  review["review"]["rating"],
                                                  review["review"]["likes"],
                                                  (review["review"]["user"]["foodie_level_num"] > 5)]
    return Review_Results

#Iterates through all restaurant IDs and scrapes
def scrape_IDs():
    list = []
    with open('ZomatoData.txt', 'rb') as f:
        data = f.readlines()
        for partial_data in data:
            dict = json.loads(partial_data)
            key = next(iter(dict))
            id = dict[key][6]
            list.append(id)
    return list
def scrape_all_review_ID():
    restIDs = ['17420003', '17419914', '17419970', '17419894', '17420079', '17419996', '17420574', '17419959', '17420067', '17420023', '17419960', '17419990', '17419947', '17420088', '17419904', '17419991', '17420501', '17420066', '17420055', '17420042', '17419899', '17419966', '17420523', '17420442', '17420487', '17420496', '17420516', '17420566', '17419953', '17420049', '17419874', '17420052', '17420078', '17419876', '17419995', '17419944', '17420515', '17420499', '17420007', '17419924', '17419965', '17420530', '17420032', '17419881', '17420730', '17420600', '17420414', '17420598', '17420569', '17420081', '17419951', '17420485', '17420691', '17420528']
    print(len(restIDs))
    recent_reviews = {}
    for rest in restIDs:
        recent_reviews[rest] = scrape_reviews_from_restaurant_id(rest)
    producer.send('ZomatoTopic2',value=recent_reviews)
print(scrape_IDs())



