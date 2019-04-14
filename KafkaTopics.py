import KafkaConsumers
from YelpRealTime import getReviewCount
from Yelp_Realtime_Scraper import scrapeYelp
import dbms
import Zomato_Scrapper

def update_yelp(df):
    print('running update yelp')
    getReviewCount()
    newReviewCount = KafkaConsumers.consume_topic1_message()
    df = dbms.initialize_yelp()
    currentReviewCount = dbms.get_restaurant_counts(df)
    scraping_list = {}
    for review in newReviewCount:
        try:
            currentReviewCount[review]
            diff = newReviewCount[review][0] - currentReviewCount[review]
            if(diff > 0):
                scraping_list[review] = [diff,newReviewCount[review][1]]
        except:
            print('match not found for: ' + str(review))
    review_dict = scrapeYelp(scraping_list)
    print(review_dict)
    df = dbms.add_rows(df,review_dict)
    dbms.save_dbms(df,True)

def update_zomato(df):
    Zomato_Scrapper.scrape_all_review_ID()
    ReviewIDs = KafkaConsumers.consume_topic2_message()
    print(ReviewIDs)

df = dbms.initialize_yelp()
update_zomato(df)