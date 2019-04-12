from dbms import initialize_yelp, yelp_id_restaurant_dict, avg_rating_binned
import pandas as pd
import random
import matplotlib.pyplot as plt
import seaborn as sns


# get avg rating for each restaurant in dataframe df
def get_res_avg_rating(df):
    df = df.groupBy("restaurant_id").avg("rating").toPandas()
    df = df.set_index("restaurant_id")
    return df.to_dict()["avg(rating)"]

# get rating trends for all restaurants in id_restaurant_dict
def get_all_res_review_trends(df, id_restaurant_dict, interval_length):
    trends = []
    for res_id, res_name in id_restaurant_dict.items():
        trend = avg_rating_binned(df, res_id, interval_length)
        trends.append(trend)
        print(trend)
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
    time = range(res_review_trends[0])
    for i in range(res_review_trends):
        plt.plot(time, res_review_trends[i], color='g')
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

    print(res_review_trends)

    # visualize the average review bar graph
    # visualize_avg_review_bar_graph(avg_rating_dict, yelp_id_restaurant)

    # visualize the restaurant review trends graph
    # visualize_res_review_trends_graph(res_review_trends)


visualize()
