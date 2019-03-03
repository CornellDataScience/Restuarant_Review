import GetRestaurantsYelpAPI
# use yelp api to
fullDict = getRestaurants('Ithaca')
for elem in fullDict:
    print(elem + ' - ' + fullDict[elem])