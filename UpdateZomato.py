from KafkaTopics import update_zomato
import dbms

df = dbms.initialize_zomato()
update_zomato(df)