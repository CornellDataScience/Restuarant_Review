# Run following line in terminal:
# FLASK_APP=frontend.py FLASK_DEBUG=1 python -m flask run

from flask import Flask, Response, request, render_template, jsonify
import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns
from dbms_visualization import get_all_res_review_trends, get_one_res_review_trend, choose_res_ids
from dbms import yelp_id_restaurant_dict, get_res_avg_rating, yelp_rest_name_to_id_dict
import io
import collections
import base64
from scipy.interpolate import spline
import numpy as np
import requests
from matplotlib.lines import Line2D

app = Flask(__name__)
yelp_df = None
zomato_df = None


'''
First send DataFrame in json format via POST to /data 
'''

@app.route("/")
def index():
	return render_template("index.html")

@app.route("/get_data", methods=['GET'])
def get_data():
	if yelp_df is not None:
		return jsonify(yelp_rest_name_to_id_dict(yelp_df))
	return jsonify({})

@app.route("/data", methods=['POST'])
def data():
	global yelp_df
	global zomato_df
	data = json.loads(request.data)
	yelp = data["yelp"]
	zomato = data["zomato"]
	yelp_df = pd.read_json(yelp, orient='split')
	zomato_df = pd.read_json(zomato,orient='split')
	print(yelp_df)
	return Response("Success")

@app.route("/img_data", methods=['GET'])
def get_img_data():
	rest_id = request.args.get('id')
	d = img_data(rest_id)
	return jsonify(img_data(rest_id))

if __name__ == "__main__":
    app.run(debug=True, threaded=True)
    WSGIServer(('', 5000), app, log=app.logger).serve_forever()
<<<<<<< HEAD
=======

>>>>>>> 2292d02c8976be54041bc6bee16e13c4330eb268

def img_data(rest_id):
	rating_dict = get_res_avg_rating(yelp_df)
	id_restaurant = yelp_id_restaurant_dict(yelp_df)
	visualize_avg_review_bar_graph(rating_dict,id_restaurant)
	img = io.BytesIO()
	plt.savefig(img, format='png')
	img.seek(0)
	plot_url = base64.b64encode(img.getvalue()).decode()
	plt.close()

	res_review_trends = []
	l_dict = collections.Counter(id_restaurant).most_common(10)
	list_res = []
	for i in range(len(l_dict)):
		list_res.append(l_dict[i][0])
	list_rest_ids = choose_res_ids(list_res, id_restaurant)
	for i in range(len(list_rest_ids)):
		res_review_trends.append(get_one_res_review_trend(yelp_df, list_rest_ids[i], id_restaurant, 'M'))
	visualize_res_review_trends_graph(res_review_trends)
	img1 = io.BytesIO()
	plt.savefig(img1,format='png')
	img1.seek(0)
	plot_url1 = base64.b64encode(img1.getvalue()).decode()
	plt.close()

	visualize_yelp_competitor_score(rest_id, yelp_df)
	img2 = io.BytesIO()
	plt.savefig(img2,format='png')
	img2.seek(0)
	plot_url2= base64.b64encode(img2.getvalue()).decode()
	plt.close()

	return {"avg_rev":plot_url, "res_rev": plot_url1, "comp_score": plot_url2}


def visualize_avg_review_bar_graph(dict, id_restaurant):
	names = []
	for key in dict.keys():
		names.append(id_restaurant[key])
		values = list(dict.values())
	plt.title("bar graph for average review for restaurants")
	plt.bar(range(len(dict)), values, tick_label=names)
	# plt.savefig('avg_review_bar_graph.png')
		# plt.show()

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


def visualize_yelp_competitor_score(res_id, yelp_df):
    # get the category of the res_id
    key = 'LbvSyP2tUSgED1yADTNYFjUd3GoagjPdCmjxx-bnx_wFMXsRxCpZ1MwYlCCYV3n8XeXhU1JFxYsOvKau9XQzMGba1UEW3FZlv2LCYLKJ5CYIu-8qEab1P243KsloXHYx'
    endpoint = 'https://api.yelp.com/v3/businesses/' + res_id
    head = {'Authorization': 'bearer %s' % key}

    r = requests.get(url=endpoint, headers=head)
    data = r.json()
    print(data)
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
                  'limit': 10,
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
    avg_rating_dict = get_res_avg_rating(yelp_df)
    competitors_rating_dict = {}
    # get the competitor rating
    for business_id in competitors:
        if business_id in avg_rating_dict:
            competitors_rating_dict[business_id] = avg_rating_dict[business_id]
    # draw the graph with rest_id high lighted in bar graph
    names = []
    for key in competitors_rating_dict.keys():
        names.append(competitors[key])
    values = list(competitors_rating_dict.values())
    colors = ['cyan'] * len(competitors_rating_dict.keys())
    colors[0] = 'blue'
    plt.title("bar graph for average review for restaurants")
    plt.bar(range(len(competitors_rating_dict)), values, color=colors)

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

def visualize_review_score():
	scoreDict = totalScores()
	names = scoreDict.keys()
	values = list(scoreDict.values())
	plt.title("bar graph for NLP review scores of restaurants")
	plt.bar(range(len(scoreDict)), values, tick_label=names)


