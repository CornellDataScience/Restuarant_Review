from KafkaTopics import update_yelp
import dbms

df = dbms.initialize_yelp()
update_yelp(df)