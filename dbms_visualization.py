from dbms import initialize_yelp, yelp_id_restaurant_dict, avg_rating_binned, get_res_avg_rating
import numpy as np
import matplotlib.pyplot as plt
import collections
from scipy.interpolate import spline
import requests
from matplotlib.lines import Line2D
from scorer import totalScores, competitor_score_over_time


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
    # plt.show()


# shows graph for review trends for restaurants
def visualize_res_competitor_review_trend(res_id):
    pass

def visualize_res_review_trends_graph(res_review_trends):
    # find the max length for time

    max_time_length = 0
    max_time_indices = []
    colors = ['b'] * (len(res_review_trends)-1)
    colors.append('c')
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
        if i == len(res_review_trends)-1:
            plt.plot(max_time_indices_new, trend_smoothed, color='c')
        else:
            plt.plot(max_time_indices_new, trend_smoothed, color='b')



    plt.xlabel('Time')
    plt.ylabel('Average Rating')
    plt.title('Review Trends for Restaurants')
    # plt.show()


# 

    # visualize the average review bar graph
    # visualize_avg_review_bar_graph(avg_rating_dict, yelp_id_restaurant)

    # visualize the restaurant review trends graph
    # visualize_res_review_trends_graph(res_review_trends)

# def visualize_selected():
#     yelp_df = initialize_yelp()

#     # generate a id to restaurant list
#     yelp_id_restaurant = yelp_id_restaurant_dict(yelp_df)

#     # yelp_df is a dataFrame containing all data in yelp
#     avg_rating_dict = get_res_avg_rating(yelp_df)

#     # selecting first 10 restaurants in yelp_df (can choose others)
#     l_dict = collections.Counter(yelp_id_restaurant).most_common(3)
#     list_res = []
#     for i in range(len(l_dict)):
#         list_res.append(l_dict[i][0])

#     # checks if the chosen restaurant ids are in the database
#     list_rest_ids = choose_res_ids(list_res, yelp_id_restaurant)

#     # creates the trends for the selected restaurant ids
#     res_review_trends = []
#     for i in range(len(list_rest_ids)):
#         res_review_trends.append(get_one_res_review_trend(yelp_df, list_rest_ids[i], yelp_id_restaurant, 'M'))

#     # visualizes the trends
#     visualize_res_review_trends_graph(res_review_trends)


# visualize the competitor's score and score of the restaurant with res_id in a bar graph
def visualize_yelp_competitor_score(res_id, yelp_df):
    # get the category of the res_id
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
                  'limit': 8,
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

    # visualize the data
    # yelp_df = initialize_yelp()
    avg_rating_dict = get_res_avg_rating(yelp_df)
    competitors_rating_dict = {}
    # get the competitor rating
    for business_id in competitors:
        if business_id in avg_rating_dict:
            competitors_rating_dict[business_id] = avg_rating_dict[business_id]
    # draw the graph with rest_id high lighted in bar graph
    names = []
    for key in competitors_rating_dict:
        names.append(key)
    values = list(competitors_rating_dict.values())
    colors = ['cyan'] * len(competitors_rating_dict.keys())
    colors[0] = 'blue'
    plt.title("bar graph for average review for restaurants")
    plt.bar(range(len(competitors_rating_dict)), values, color=colors)
    plt.savefig('competitor bar graph.png')

    # add legends
    custom_lines = [Line2D([0], [0], color='blue', lw=4)]
    for i in range(len(competitors_rating_dict.keys()) - 1):
        custom_lines.append(Line2D([0], [0], color='cyan', lw=4))
    competitor_names = []
    i = 0
    for res_id in competitors_rating_dict.keys():
        competitor_names.append(str(i) + ': ' + competitors[res_id])
        i += 1

    plt.legend(custom_lines, competitor_names, fontsize='small', loc='upper center', bbox_to_anchor=(0.5, -0.03), ncol=3)
    # plt.show()


# visualize the NLP review scores of restaurants
def visualize_review_score(res_id):
    scoreDict = totalScores(res_id)
    print("scoredict" + str(scoreDict))
    names = []
    for elem in scoreDict:
        names.append(elem)
    values = list(scoreDict.values())
    plt.title("bar graph for NLP category scores of restaurant")
    plt.bar(range(len(scoreDict)), values, tick_label=names)
    # plt.savefig('avg_NLP_review_score_bar_graph.png')
    # plt.show()

# mock up function of all_score_over_time
def all_score_over_time(a):
    return {'a' : [0, 1], 'b' : [0, 1, 2, 3]}


# visualize the NLP review trends of restaurants
def visualize_NLP_review_trends(res_id,category):
    res_review_trends = competitor_score_over_time(res_id,category)
    print("got to here")
    max_time_length = 0
    colors = []
    for cat in res_review_trends:
        colors.append('cyan')
    colors[0] = 'blue'
    for trend in res_review_trends.values():
        if max_time_length < len(trend):
            max_time_length = len(trend)
    
    # plot the graph
    temp = np.arange(max_time_length)
    max_time_indices_new = np.linspace(temp.min(), temp.max(), 300)
    for trend in res_review_trends.values():
        buffer = np.zeros(max_time_length - len(trend))
        trend = np.append(buffer, trend)
        trend_smoothed = spline(temp, trend, max_time_indices_new)
        plt.plot(max_time_indices_new, trend_smoothed)

    plt.xlabel('Time')
    plt.ylabel('Average NLP Review Rating')
    plt.title('NLP Review Trends for Restaurants')
    # plt.show()
# visualize_review_score('3QCY93kLbH29Cctus8IyKQ')
# visualize_NLP_review_trends('3QCY93kLbH29Cctus8IyKQ', "food")

# yelp_df = initialize_yelp()
# id_restaurant = yelp_id_restaurant_dict(yelp_df)
# res_review_trends = []
# l_dict = collections.Counter(id_restaurant).most_common(10)
# list_res = []
# for i in range(len(l_dict)):
#     list_res.append(l_dict[i][0])
# list_rest_ids = choose_res_ids(list_res, id_restaurant)
# for i in range(len(list_rest_ids)):
#     res_review_trends.append(get_one_res_review_trend(yelp_df, list_rest_ids[i], id_restaurant, 'M'))
# visualize_res_review_trends_graph(res_review_trends)








