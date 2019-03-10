import json
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import os
from selenium import webdriver
import time

def scrapeZomato(url):
    cwd = os.getcwd()
    driverPath = cwd + '/chromedriver'
    driver = webdriver.Chrome(executable_path=driverPath)
    driver.get(url)
    time.sleep(3)
    elem = driver.find_element_by_xpath("//div[@class ='ui segment clearfix zs-load-more res-page-load-more']")
    while(elem != None):

        try:
            elem.click()

            elem = driver.find_element_by_xpath("//div[@class ='ui segment clearfix zs-load-more res-page-load-more']")
        except:
            print('Not found')
            break
    soup = BeautifulSoup(driver.page_source, 'lxml')
    ReviewDict = {}
    currentRestaurant = soup.find('a',class_='ui large header left').text
    currentRestaurant = currentRestaurant.replace('\n','').strip()
    for fullReview in soup.find_all('div', class_='ui segments res-review-body res-review clearfix js-activity-root mbti item-to-hide-parent stupendousact'):
        ReviewID = int(fullReview['data-review_id'])
        ReviewDict[ReviewID] = ['Yelp', currentRestaurant]
        review = fullReview.findChild('div',class_='rev-text mbot0')
        if(review != None):
            reviewText = review.text
            reviewText = reviewText.replace('POSITIVE', '')
            reviewText = reviewText.replace('Rated', '')
            reviewText = reviewText.strip()
        else:
            reviewText = ''
        dateDiv = fullReview.findChild('div',class_='fs12px pbot0 clearfix',recursive=True)
        timeTag = dateDiv.findChild('time',recursive=True)
        ReviewDict[ReviewID].append(timeTag['datetime'])
        ReviewDict[ReviewID].append(reviewText)
        ratingDiv = fullReview.findChild('div', class_='ttupper fs12px left bold zdhl2 tooltip icon-font-level-7')
        if (ratingDiv != None):
            ratingText = ratingDiv['aria-label']
            ratingText = ratingText.replace('Rated', '')
            ratingText = ratingText.strip()
            rating = float(ratingText)
            ReviewDict[ReviewID].append(rating)
        else:
            ReviewDict[ReviewID].append(None)
        voteDiv = fullReview.findChild('div',class_='left mr5 ui tiny labeled button js-btn-thank',recursive=True)
        voteCount  = 0
        if voteDiv != None:
            voteCount = voteDiv['data-likes']
            if(voteCount == ''):
                voteCount = 0
            else:
                voteCount = int(voteCount)
        ReviewDict[ReviewID].append(voteCount)
        #fullReview.findChild('span')

    # close the browser window
    driver.close()
    return ReviewDict

def getRestaurants():
    baseURL = 'https://www.zomato.com/finger-lakes-region-ny/ithaca-restaurants?page='
    allRestaurants = []

    cwd = os.getcwd()
    driverPath = cwd + '/chromedriver'
    driver = webdriver.Chrome(executable_path=driverPath)

    for i in range(1,16):

        baseURL += str(i)
        driver.get(baseURL)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source,'lxml')
        for restaurant in soup.find_all('a',class_= 'result-title hover_feedback zred bold ln24 fontsize0'):
            print(restaurant['href'])
        baseURL = 'https://www.zomato.com/finger-lakes-region-ny/ithaca-restaurants?page='



# getRestaurants()
# scrapeZomato('https://www.zomato.com/finger-lakes-region-ny/collegetown-bagels-3-ithaca')


