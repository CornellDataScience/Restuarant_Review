import KafkaConsumers
from YelpRealTime import getReviewCount
from Yelp_Realtime_Scraper import scrapeYelp
import dbms
import Zomato_Scrapper
import datetime

def update_yelp(df):
    print('Updating at: ' + str(datetime.datetime.now()))
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
            pass
    review_dict = scrapeYelp(scraping_list)
    print('New Reviews found: '+ str(review_dict))
    df = dbms.add_rows(df,review_dict)
    dbms.save_yelp(df)

def update_zomato(df):
    print('Updating at: ' + str(datetime.datetime.now()))
    Zomato_Scrapper.scrape_all_review_ID()
    # print('reviews scraped and sent')
    ReviewIDs = KafkaConsumers.consume_topic2_message()
    recent_review_dict = dbms.get_top_5_review_ids(df)
    ReviewDict = {}
    print('Comparing Reviews')
    for rest in ReviewIDs:
        countToScrape = 0
        if(len(ReviewIDs.get(rest)) > 0):

            IDList = ReviewIDs.get(rest)
            lenList = len(IDList)
            lastID = IDList[lenList-1]
            currentReviewList = recent_review_dict.get(rest)
            for id in currentReviewList:
                if(id == lastID):
                    break
                countToScrape += 1
            try:
                ReviewDict.update(Zomato_Scrapper.scrape_latest_reviews(countToScrape,rest))
            except:
                print('didnt scrape reviews for:')
                print(rest)
    print('New Reviews found: '+ str(ReviewDict))
    df = dbms.add_rows(df,ReviewDict)
    dbms.save_zomato(df,ReviewDict)
