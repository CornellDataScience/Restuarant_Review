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
        print('match found')
        print('review difference is current - new')
        print(currentReviewCount[review])
        print(newReviewCount[review][0])
    except:
        print('match not found for: ')
        print(review)
