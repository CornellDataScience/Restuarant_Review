from bs4 import BeautifulSoup
import requests
from lxml.html import fromstring
from concurrent.futures import ThreadPoolExecutor
import json



## Initial benchmark execution time: approximately 17 seconds
## Current execution time: approximately 8 seconds with ThreadPoolExecutor

def scrapeYelp(restaurants):
    ReviewDict = {}
    pages = {}
    for restId in restaurants:
        numPages = int(restaurants[restId][0]/20)
        if(restaurants[restId][0] > 20):
            pages[restaurants[restId][1]] = [20,restId]
            r = requests.get(restaurants[restId][1])
            soup = BeautifulSoup(r.content,'lxml')
            page_Divs = soup.find_all('div', class_='arrange_unit page-option')
            restCount = restaurants[restId][0] - 20
            for div in page_Divs:
                if (restCount <= 20):
                    pages[div.findChild('a')['href']] = [restCount,restId]
                    break
                else:
                    pages[div.findChild('a')['href']] =[20,restId]
                restCount-=20
        else:
            pages[restaurants[restId][1]] = [restaurants[restId][0],restId]
    return scrape_yelp_page(pages)

# start = time.time()

# with open('YelpData.txt', 'w') as outfile:
#     json.dump(dict, outfile
# end = time.time()
# print(end s- start)

def scrape_yelp_page(pages):
    ReviewDict = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for link in pages:
            futures.append([executor.submit(requests.get,link),pages[link][0],pages[link][1]])

        #figure out how to also hold information for rerestaurantid and number of reviews to be scraped
    for future in futures:
        numrestScraped = 0
        r = future[0].result()
        soup = BeautifulSoup(r.content, 'lxml')
        name_div = soup.find('div', class_='biz-page-header clearfix')

        try:
            end_div = name_div.findChild('div', class_='u-inline-block')
            currentRestaurant = name_div.findChild('h1', recursive=True).text + ' ' + end_div.findChild('h1').text
        except:
            currentRestaurant = 'couldnt find'
        for fullReview in soup.find_all('div', class_='review review--with-sidebar'):
            review = fullReview.findChild('div', class_='review-content', recursive=True)
            sidebar = fullReview.findChild('div', class_='review-sidebar', recursive=True)
            ReviewIDFull = sidebar.findChild('a', class_='arrange arrange--middle send-to-friend', recursive=True)[
                'data-pop-uri']
            ReviewID = ReviewIDFull.replace('/send_to_friend/review/', '')
            ReviewDict[ReviewID] = ['Yelp', currentRestaurant]
            dateText = review.findChild('span', recursive=True).text
            dateText = dateText.replace('\n', '')
            dateText = dateText.replace('Updated review', '')
            dateText = dateText.strip()
            ReviewDict[ReviewID].append(dateText)
            reviewText = review.findChild('p', recursive=True).text
            reviewText = reviewText.replace('\xa0', '')
            ReviewDict[ReviewID].append(reviewText)
            ratingDiv = review.findChild('div', class_='biz-rating biz-rating-large clearfix')
            rating = ratingDiv.findChild('div', recursive=False).findChild('div', recursive=False)
            if (rating['title'] == '5.0 star rating'):
                ReviewDict[ReviewID].append(5)
            elif (rating['title'] == '4.0 star rating'):
                ReviewDict[ReviewID].append(4)
            elif (rating['title'] == '3.0 star rating'):
                ReviewDict[ReviewID].append(3)
            elif (rating['title'] == '2.0 star rating'):
                ReviewDict[ReviewID].append(2)
            else:
                ReviewDict[ReviewID].append(1)
            voteCount = 0
            voteDiv = fullReview.findChild('div', class_='rateReview voting-feedback', recursive=True)
            fullVote = voteDiv.findChildren('li', class_='vote-item inline-block', recursive=True)
            totalVotes = 0
            for vote in fullVote:
                voteType = vote.findChild('span', class_='vote-type', recursive=True).text
                if voteType == 'Useful':
                    voteCount = vote.findChild('span', class_='count', recursive=True).text
                    if (voteCount != ''):
                        totalVotes += int(voteCount)
                elif (voteType == 'Funny'):
                    voteCount = vote.findChild('span', class_='count', recursive=True).text
                    if (voteCount != ''):
                        totalVotes += int(voteCount)
                elif (voteType == 'Cool'):
                    voteCount = vote.findChild('span', class_='count', recursive=True).text
                    if (voteCount != ''):
                        totalVotes += int(voteCount)
            ReviewDict[ReviewID].append(totalVotes)
            ReviewDict[ReviewID].append(future[2])
            numrestScraped += 1
            if (numrestScraped == future[1]):
                break
    return ReviewDict

dict = scrapeYelp({'P1aQqll76KRvZHdZ8jaQvQ': [42, 'https://www.yelp.com/biz/the-rook-ithaca?sort_by=date_desc'], 'FJsh0TOIQJWj3aQP4Yg0_A': [2, 'https://www.yelp.com/biz/saigon-kitchen-ithaca?sort_by=date_desc']})
print(dict)