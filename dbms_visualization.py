from dbms import initialize_yelp, yelp_id_restaurant_dict
import pandas as pd
import random
import matplotlib.pyplot as plt
import seaborn as sns


# get avg rating for each restaurant in dataframe df
def get_res_avg_rating(df):
    df = df.groupBy("restaurant_id").avg("rating").toPandas()
    df = df.set_index("restaurant_id")
    return df.to_dict()["avg(rating)"]


# get rating trends for restaurant with id_restaurant in dataframe df
def get_res_review_trend(df, restaurant_id):
    df = df.select(["restaurant"])

    return 0

# get rating trends for all restaurants in id_restaurant_dict
def get_all_res_review_trends(df, id_restaurant_dict):
    trends = []
    for id, name in id_restaurant_dict.items():
        trends.append(get_res_review_trend(df, id))
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
def visualize_res_review_trends_graph():
    return 0


def visualize():
    yelp_df = initialize_yelp()
    # yelp_df is a dataFrame containing all data in yelp
    avg_rating_dict = get_res_avg_rating(yelp_df)

    res_review_trends = get_res_review_trends(yelp_df)


    # generate a id to restaurant list
    yelp_id_restaurant = yelp_id_restaurant_dict(yelp_df)


    # visualize_avg_review_bar_graph(avg_rating_dict, yelp_id_restaurant)
    # visualize_res_review_trends_graph()


visualize()
