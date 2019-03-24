# FLASK_APP=frontend.py FLASK_DEBUG=1 python -m flask run
from flask import Flask, Response, request
import pandas as pd
import json
app = Flask(__name__)

yelp_df = None
zomato_df = None

@app.route("/")
def index():
    return "Hello World!"

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

