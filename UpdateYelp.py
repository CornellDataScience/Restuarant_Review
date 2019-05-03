from KafkaTopics import update_yelp
import dbms
from post_to_frontend import send_data


yelp_df = dbms.initialize_yelp()
update_yelp(yelp_df)

zomato_df = dbms.initialize_zomato()
send_data(yelp_df, zomato_df)




