from dbms import initialize_yelp, yelp_id_restaurant_dict, avg_rating_binned, get_res_avg_rating
import pandas as pd
import random
import numpy as np
import math
import matplotlib.pyplot as plt

# get rating trends for all restaurants in id_restaurant_dict
def get_all_res_review_trends(df, id_restaurant_dict, interval_length):
    trends = []
    for res_id, res_name in id_restaurant_dict.items():
        trend = avg_rating_binned(df, res_id, interval_length)
        trends.append(trend)
    return trends

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
        if max_time_length < len(res_review_trends[i].index.values):
            max_time_indices = res_review_trends[i].index.values.astype(str)
            max_time_length = len(res_review_trends[i].index.values)

    for i in range(len(res_review_trends)):
        trend = res_review_trends[i]['rating'].values
        buffer = np.zeros(max_time_length - len(trend))
        trend = np.append(buffer, trend)
        plt.plot(max_time_indices, trend, color=list(np.random.choice(range(256), size=3) / 255))

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


visualize()
