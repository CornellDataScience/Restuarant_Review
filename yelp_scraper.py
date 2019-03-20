from bs4 import BeautifulSoup
import requests
from lxml.html import fromstring
import time
import json
from concurrent.futures import ThreadPoolExecutor


## Initial benchmark execution time: approximately 17 seconds
## Current execution time: approximately 8 seconds with ThreadPoolExecutor

def scrapeYelp(url,id):
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'lxml')
    page_Divs = soup.find_all('div',class_='arrange_unit page-option')
    page_links = []
    page_links.append(url)
    for div in page_Divs:
        page_links.append(div.findChild('a')['href'])

    name_div = soup.find('div',class_='biz-page-header clearfix')
    try:
        end_div = name_div.findChild('div',class_='u-inline-block')
        currentRestaurant = name_div.findChild('h1',recursive = True).text + ' ' +end_div.findChild('h1').text
    except:
        currentRestaurant = 'couldnt find'
    ReviewDict = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(requests.get, link) for link in page_links]  
    for future in futures:
        r = future.result()
        soup = BeautifulSoup(r.content,'lxml')             
        for fullReview in soup.find_all('div',class_='review review--with-sidebar'):
            review = fullReview.findChild('div',class_='review-content',recursive=True)
            sidebar = fullReview.findChild('div', class_='review-sidebar',recursive=True)
            ReviewIDFull = sidebar.findChild('a',class_='arrange arrange--middle send-to-friend', recursive=True)['data-pop-uri']
            ReviewID = ReviewIDFull.replace('/send_to_friend/review/','')            
            ReviewDict[ReviewID] = ['Yelp',currentRestaurant]
            dateText = review.findChild('span', recursive = True).text
            dateText = dateText.replace('\n','')
            dateText = dateText.replace('Updated review','')
            dateText = dateText.strip()
            ReviewDict[ReviewID].append(dateText)
            reviewText = review.findChild('p',recursive=True).text
            reviewText = reviewText.replace('\xa0','')
            ReviewDict[ReviewID].append(reviewText)
            ratingDiv = review.findChild('div',class_='biz-rating biz-rating-large clearfix')
            rating = ratingDiv.findChild('div',recursive=False).findChild('div',recursive=False)
            if(rating['title'] == '5.0 star rating'):
                ReviewDict[ReviewID].append(5)
            elif(rating['title'] == '4.0 star rating'):
                ReviewDict[ReviewID].append(4)
            elif(rating['title'] == '3.0 star rating'):
                ReviewDict[ReviewID].append(3)
            elif(rating['title'] == '2.0 star rating'):
                ReviewDict[ReviewID].append(2)
            else:
                ReviewDict[ReviewID].append(1)
            voteCount = 0
            voteDiv = fullReview.findChild('div',class_='rateReview voting-feedback',recursive=True)
            fullVote = voteDiv.findChildren('li',class_='vote-item inline-block',recursive=True)
            totalVotes = 0
            for vote in fullVote:
                voteType = vote.findChild('span',class_='vote-type',recursive=True).text
                if voteType == 'Useful':
                    voteCount = vote.findChild('span',class_='count',recursive=True).text
                    if(voteCount != ''):
                        totalVotes += int(voteCount)
                elif(voteType == 'Funny'):
                    voteCount = vote.findChild('span',class_='count',recursive=True).text
                    if(voteCount != ''):
                        totalVotes += int(voteCount)
                elif(voteType == 'Cool'):
                    voteCount = vote.findChild('span',class_='count',recursive=True).text
                    if(voteCount != ''):
                        totalVotes += int(voteCount)
            ReviewDict[ReviewID].append(totalVotes)
            ReviewDict[ReviewID].append(id)
    return ReviewDict
# start = time.time()
# end = time.time()
# print(end s- start)
