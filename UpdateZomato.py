from KafkaTopics import update_zomato
import dbms
from post_to_frontend import send_data


zomato_df = dbms.initialize_zomato()
update_zomato(zomato_df)

yelp_df = dbms.initialize_yelp()
send_data(yelp_df, zomato_df)




