# Run following line in terminal:
# FLASK_APP=frontend.py FLASK_DEBUG=1 python -m flask run

from flask import Flask, Response, request
import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns
from dbms_visualization import get_all_res_review_trends, get_one_res_review_trend, choose_res_ids
from dbms import yelp_id_restaurant_dict, get_res_avg_rating
import io
import collections
import base64
import numpy as np

app = Flask(__name__)
yelp_df = None
zomato_df = None


'''
First send DataFrame in json format via POST to /data 
'''
@app.route("/")
def index():
	if yelp_df is None or zomato_df is None:
		return Response("No data available.")
	### plot 
	rating_dict = get_res_avg_rating(yelp_df)
	id_restaurant = yelp_id_restaurant_dict(yelp_df)
	names = []
	for key in rating_dict.keys():
	    names.append(id_restaurant[key])
	values = list(rating_dict.values())
	plt.title("bar graph for average review for restaurants")
	plt.bar(range(len(rating_dict)), values, tick_label=names)
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
	img1 = io.BytesIO()
	plt.savefig(img1,format='png')
	img1.seek(0)
	plot_url1 = base64.b64encode(img1.getvalue()).decode()
	return '<img src="data:image/png;base64,{}"> <img src="data:image/png;base64,{}">'.format(plot_url, plot_url1)

@app.route("/data", methods=['POST'])
def data():
	global yelp_df
	global zomato_df
	data = json.loads(request.data)

	yelp = data["yelp"]
	zomato = data["zomato"]
	yelp_df = pd.read_json(yelp, orient='split')
	zomato_df = pd.read_json(zomato,orient='split')
	print(zomato_df)
	return Response("Success")

if __name__ == "__main__":
    app.run(debug=True, threaded=True)
    WSGIServer(('', 5000), app, log=app.logger).serve_forever()


# def visualize_avg_review_bar_graph(dict, id_restaurant):
#     names = []
#     for key in dict.keys():
#         names.append(id_restaurant[key])
#     values = list(dict.values())
#     plt.title("bar graph for average review for restaurants")
#     plt.bar(range(len(dict)), values, tick_label=names)
#     plt.savefig('avg_review_bar_graph.png')
#     plt.show()

