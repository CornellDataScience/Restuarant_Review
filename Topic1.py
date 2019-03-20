from Topic1Consumer import consume_message
from YelpRealTime import getReviewCount
from Yelp_Realtime_Scraper import scrapeYelp
import dbms

getReviewCount()
print('reviews produced')
newReviewCount = consume_message()
print('reviews consumed')
df = dbms.initialize_dbms()
currentReviewCount = dbms.get_restaurant_counts(df)
dbms.save_dbms(df)
print(newReviewCount)
print(' ')
print(currentReviewCount)
scraping_list = {}
for review in newReviewCount:
    try:
        currentReviewCount[review]
        diff = newReviewCount[review][0] - currentReviewCount[review]
        if(diff > 0):
            print(review + ':      ' + str(diff))
            scraping_list[review] = [diff,newReviewCount[review][1]]
        else:
            print('dbms up to date ')
    except:
        print('match not found for: ' + str(review))
print(scraping_list)
