import requests
from dbms import initialize_yelp, initialize_zomato
import json 

yelp_df = initialize_yelp()
zomato_df = initialize_zomato()
print(yelp_df)
yelp_json = yelp_df.to_json(orient='split')
zomato_json = zomato_df.to_json(orient='split')

data = json.dumps({"yelp":yelp_json, "zomato": zomato_json})
r = requests.post("http://localhost:5000/data",data=data)
print(r.text)
