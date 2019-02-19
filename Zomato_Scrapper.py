import json
import requests

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


# Order of Json String Dictionary: {ReviewID: [ReviewSiteName, RestaurantName, ReviewDate, ReviewText, ReviewRating, ReviewVotes, isElite]}
url = 'https://developers.zomato.com/api/v2.1/search?entity_id=137164&entity_type=subzone'


#  &start=20
response_json = requests.get(url, headers={'user-key': '4dded5ab75a73b4c37bf996ffd3e1a5b'})

restaurants_num = json.loads(response_json.text)["results_found"]

restaurants_list = json.loads(response_json.text)["restaurants"]

# fetch all the restaurants up to 100
for i in range(20, 100, 20):
    new_url = url + "&start=" + str(i)
    # print(new_url)
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
print(review_list)