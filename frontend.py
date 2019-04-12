# Run following line in terminal:
# FLASK_APP=frontend.py FLASK_DEBUG=1 python -m flask run

from flask import Flask, Response, request
import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns
from dbms import yelp_id_restaurant_dict, get_res_avg_rating
import io
import base64

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

	rating_dict = get_res_avg_rating(yelp_df)
	id_restaurant = yelp_id_restaurant_dict(yelp_df)
	names = []
	for key in rating_dict.keys():
	    names.append(id_restaurant[key])
	values = list(rating_dict.values())
	plt.title("bar graph for average review for restaurants")
	plt.bar(range(len(rating_dict)), values, tick_label=names)
	plt.savefig(img, format='png')
	img.seek(0)
	plot_url = base64.b64encode(img.getvalue()).decode()
	return '<img src="data:image/png;base64,{}">'.format(plot_url)

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

