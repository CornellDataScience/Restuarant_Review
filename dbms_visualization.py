from dbms import initialize_yelp, yelp_id_restaurant_dict, avg_rating_binned, get_res_avg_rating
import pandas as pd
import random
import numpy as np
import math
import matplotlib.pyplot as plt
import collections
from scipy.interpolate import spline

# get rating trends for all restaurants in id_restaurant_dict
def get_all_res_review_trends(df, id_restaurant_dict, interval_length):
    trends = []
    for res_id, res_name in id_restaurant_dict.items():
        trend = avg_rating_binned(df, res_id, interval_length)
        trends.append(trend)
    return trends

# get rating trend for one restaurant
def get_one_res_review_trend(df, res_id, id_restaurant_dict, interval_length):
    if res_id in id_restaurant_dict.keys():
        trend = avg_rating_binned(df, res_id, interval_length)
    return trend

# given a list of restaurant ids, checks if they are present in database and returns the list
# of restaurant ids in database
def choose_res_ids(rest_ids, id_restaurant_dict):
    list_rest_ids = []
    for i in range(len(rest_ids)):
        if rest_ids[i] in id_restaurant_dict.keys():
            list_rest_ids.append(rest_ids[i])
    return list_rest_ids

# shows bar graph for average review
def visualize_avg_review_bar_graph(dict, id_restaurant):
    names = []
    for key in dict.keys():
        names.append(id_restaurant[key])
    values = list(dict.values())
    plt.title("bar graph for average review for restaurants")
    plt.bar(range(len(dict)), values, tick_label=names)
    plt.savefig('avg_review_bar_graph.png')
    plt.show()


# shows graph for review trends for restaurants
def visualize_res_review_trends_graph(res_review_trends):
    # find the max length for time
    max_time_length = 0
    max_time_indices = []
    for i in range(len(res_review_trends)):
        if max_time_length < len(res_review_trends[i][0].index.values):
            max_time_indices = res_review_trends[i][0].index.values.astype(str)
            max_time_length = len(res_review_trends[i][0].index.values)

    # plot the graph
    temp = np.arange(len(max_time_indices))
    max_time_indices_new = np.linspace(temp.min(), temp.max(), 300)
    for i in range(len(res_review_trends)):
        trend = res_review_trends[i][0]['rating'].values
        buffer = np.zeros(max_time_length - len(trend))
        trend = np.append(buffer, trend)
        trend_smoothed = spline(temp, trend, max_time_indices_new)
        plt.plot(max_time_indices_new, trend_smoothed, color=list(np.random.choice(range(256), size=3) / 255))

    plt.xlabel('Time')
    plt.ylabel('Average Rating')
    plt.title('Review Trends for Restaurants')
    plt.show()

def visualize():
    yelp_df = initialize_yelp()

    # generate a id to restaurant list
    yelp_id_restaurant = yelp_id_restaurant_dict(yelp_df)

    # yelp_df is a dataFrame containing all data in yelp
    avg_rating_dict = get_res_avg_rating(yelp_df)

    res_review_trends = get_all_res_review_trends(yelp_df, yelp_id_restaurant, 'M')

    # visualize the average review bar graph
    # visualize_avg_review_bar_graph(avg_rating_dict, yelp_id_restaurant)

    # visualize the restaurant review trends graph
    visualize_res_review_trends_graph(res_review_trends)

def visualize_selected():
    yelp_df = initialize_yelp()
    
    # generate a id to restaurant list
    yelp_id_restaurant = yelp_id_restaurant_dict(yelp_df)
    
    # yelp_df is a dataFrame containing all data in yelp
    avg_rating_dict = get_res_avg_rating(yelp_df)
    
    # selecting first 10 restaurants in yelp_df (can choose others)
    l_dict = collections.Counter(yelp_id_restaurant).most_common(3)
    list_res = []
    for i in range(len(l_dict)):
        list_res.append(l_dict[i][0])

    # checks if the chosen restaurant ids are in the database
    list_rest_ids = choose_res_ids(list_res, yelp_id_restaurant)

    # creates the trends for the selected restaurant ids
    res_review_trends = []
    for i in range(len(list_rest_ids)):
        res_review_trends.append(get_one_res_review_trend(yelp_df, list_rest_ids[i], yelp_id_restaurant, 'M'))

    # visualizes the trends
    visualize_res_review_trends_graph(res_review_trends)

# visualize()
visualize_selected()
