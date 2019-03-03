
# coding: utf-8

# In[94]:


import requests

def getRestaurants(location):
    key = 'LbvSyP2tUSgED1yADTNYFjUd3GoagjPdCmjxx-bnx_wFMXsRxCpZ1MwYlCCYV3n8XeXhU1JFxYsOvKau9XQzMGba1UEW3FZlv2LCYLKJ5CYIu-8qEab1P243KsloXHYx'
    endpoint = 'https://api.yelp.com/v3/businesses/search'
    head = {'Authorization': 'bearer %s' % key}
    offsetCount = 50
    restaurantDict = {}
    while(True):
        parameters = { 'term': 'restaurants',
                   'limit': 50,
                   'offset':offsetCount ,
                   'radius': 40000,
                   'location': location    
        }
        r = requests.get(url = endpoint, params = parameters, headers = head)
        data = r.json()
        try:
            for business in data['businesses']:
                url = business['url']
                url = url.split('?')[0]
                url = url + '?sort_by=date_desc'
                restaurantDict[business['name']] = url
        except:
            break
        offsetCount+=50
            
    return restaurantDict
   
   
    
fullDict = getRestaurants('Ithaca')
for elem in fullDict:
    print(elem + ' - ' + fullDict[elem])

    

