import requests
from dbms import initialize_yelp, initialize_zomato
import json 

def send_data(yelp_df, zomato_df):
	yelp_json = yelp_df.to_json(orient='split')
	zomato_json = zomato_df.to_json(orient='split')
	data = json.dumps({"yelp":yelp_json, "zomato": zomato_json})
	r = requests.post("http://128.84.48.178:5000/data",data=data)
