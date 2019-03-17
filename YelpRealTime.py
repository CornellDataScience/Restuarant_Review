import requests
from time import sleep
from json import dumps
from kafka import KafkaProducer
key = 'LbvSyP2tUSgED1yADTNYFjUd3GoagjPdCmjxx-bnx_wFMXsRxCpZ1MwYlCCYV3n8XeXhU1JFxYsOvKau9XQzMGba1UEW3FZlv2LCYLKJ5CYIu-8qEab1P243KsloXHYx'
endpoint = 'https://api.yelp.com/v3/businesses/search'
head = {'Authorization': 'bearer %s' % key}
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                      dumps(x).encode('utf-8'))
def getReviewCount():
    offsetCount = 0
    restaurantDict = {}
    while (True):
        parameters = { 'term': 'restaurants',
                   'limit': 50,
                   'offset':offsetCount ,
                   'radius': 40000,
                   'location': 'Ithaca'
        }
        r = requests.get(url = endpoint, params = parameters, headers = head)
        data = r.json()
        try:
            for business in data['businesses']:
                id = business['id']
                reviewCount = business['review_count']
                url = business['url']
                url = url.split('?')[0] + '?'
                url = url + 'sort_by=date_desc'
                restaurantDict[id] = [reviewCount, url]

        except:
            break
            print('didnt work')

        offsetCount+=50
    print(restaurantDict)
    producer.send('restaurant_review', value=restaurantDict)


getReviewCount()
print('done')
# offsetCount = 0
# restaurantDict = []
# while(True):
#     parameters = { 'term': 'restaurants',
#                'limit': 50,
#                'offset':offsetCount ,
#                'radius': 40000,
#                'location': 'Ithaca'
#     }
#     r = requests.get(url = endpoint, params = parameters, headers = head)
#     data = r.json()
#     try:
#         for business in data['businesses']:
#             url = business['id']
#             restaurantDict.append(url)
#     except:
#         break
#     offsetCount+=50
