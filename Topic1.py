from Topic1Consumer import consume_message
from YelpRealTime import getReviewCount
import dbms

getReviewCount()
newReviewCount = consume_message()
df = dbms.initialize_dbms()
currentReviewCount = dbms.get_restaurant_counts(df)
print(newReviewCount)
print(' ')
print(currentReviewCount)
