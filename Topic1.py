from Topic1Consumer import consume_message
from YelpRealTime import getReviewCount
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
for review in newReviewCount:
    try:
        currentReviewCount[review]
        diff = newReviewCount[review][0] - currentReviewCount[review]
        if(diff > 0):
            print(review + ':      ' + str(diff))
        else:
            print('dbms up to date ')
    except:
        print('match not found for: ' + str(review))
