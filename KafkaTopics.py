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
    dbms.save_yelp(df)

def update_zomato(df):
    Zomato_Scrapper.scrape_all_review_ID()
    print('reviews scraped and sent')
    ReviewIDs = KafkaConsumers.consume_topic2_message()
    print('Messages from Topic')
    print(ReviewIDs)
    recent_review_dict = dbms.get_top_5_review_ids(df)
    ReviewDict = {}
    print('Comparing Reviews')
    for rest in ReviewIDs:
        countToScrape = 0
        lastID = ReviewIDs[rest][len(ReviewIDs[rest])-1]
        currentReviewList = recent_review_dict.get(rest)
        for id in currentReviewList:
            countToScrape += 1
            if(id == lastID):
                break
        ReviewDict.update(Zomato_Scrapper.scrape_latest_reviews(2,rest))
    df = dbms.add_rows(df,ReviewDict)
    dbms.save_zomato(df,ReviewDict)
df = dbms.initialize_zomato()
update_zomato(df)