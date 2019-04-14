import json
import requests
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import pandas as pd

def scrapeZomato(url):
    cwd = os.getcwd()
    driverPath = cwd + '/chromedriver'
    driver = webdriver.Chrome(executable_path=driverPath)
    driver.get(url)
    time.sleep(3)
    try:
        elem = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class ='ui segment clearfix zs-load-more res-page-load-more']"))
        )
        elem = driver.find_element_by_xpath("//div[@class ='ui segment clearfix zs-load-more res-page-load-more']")
    except TimeoutException:
        elem = None
    while(elem != None):

        try:
            elem.click()

            elem = driver.find_element_by_xpath("//div[@class ='ui segment clearfix zs-load-more res-page-load-more']")
        except:
            print('Not found')
            break
    soup = BeautifulSoup(driver.page_source, 'lxml')
    ReviewDict = {}
    restID = soup.find('div',class_='review-form-container pos-relative')['data-res_id']
    currentRestaurant = soup.find('a',class_='ui large header left').text
    currentRestaurant = currentRestaurant.replace('\n','').strip()
    for fullReview in soup.find_all('div', class_='ui segments res-review-body res-review clearfix js-activity-root mbti item-to-hide-parent stupendousact'):
        ReviewID = int(fullReview['data-review_id'])
        ReviewDict[ReviewID] = ['Zomato', currentRestaurant]
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
        ReviewDict[ReviewID].append(restID)
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

def idScraper():
    IDDict = {}
    filename = "ZomatoURLs2"
    url_file = open(filename, "r")
    for url in url_file:
        print(url)
        print('starting')
        cwd = os.getcwd()
        driverPath = cwd + '/chromedriver'
        driver = webdriver.Chrome(executable_path=driverPath)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'lxml')

        try:

            currentRestaurant= soup.find('title').text
            #currentRestaurant = currentRestaurant.strip(',')[0]
            currentRestaurant = currentRestaurant.split(',')[0]
            restIDDiv = soup.find('div',class_='quickreview__form hidden pos-relative clearfix')
            restID = restIDDiv.findChild('input',class_='file-chooser')['data-resid']
            IDDict[currentRestaurant] = restID
            print(IDDict)
            driver.close()
        except:
            print(url + ' didnt work')
    return IDDict
def addRestaurantIDs():
    idDict = {'Moosewood Restaurant': '17420003', 'Collegetown Bagels': '17419914', 'Just A Taste': '17419970',
              'The BoatYard Grill': '17419894', 'Viva Taqueria': '17420079', "Maxie's Supper Club": '17419996',
              'AGAVA Restaurant': '17420574', 'Ithaca Ale House': '17419959', 'Taste of Thai': '17420067',
              'Purity Ice Cream Co': '17420023', 'Ithaca Bakery': '17420654', "Madeline's": '17419990',
              'Glenwood Pines': '17419947', "ZaZa's Cucina": '17420088', 'Carriage House Cafe': '17419904',
              'Mahogany Grill': '17419991', 'Bandwagon Brew Pub': '17420501', 'Tamarind': '17420066',
              'Sticky Rice': '17420055', 'Shortstop Deli': '17420042', 'Cafe Dewitt': '17419899',
              'John Thomas Steakhouse': '17419966', 'Ciao': '17420523', 'Five Guys Burgers and Fries': '17420442',
              'NorthStar House': '17420487', 'Waffle Frolic': '17420496', "Kilpatrick's Publick House": '17420516',
              'Saigon Kitchen': '17420566', 'Heights Café & Grill': '17419953', 'Souvlaki House': '17420049',
              'Aladdins Natural Eatery': '17419874', 'State Diner of Ithaca': '17420052', 'Viva Cantina': '17420078',
              'Antlers': '17419876', "The Mate' Factor Cafe & Juice Bar": '17419995', 'Gimme Coffee': '17419944',
              'Mehak: Authentic Indian Cuisine': '17420515', 'Buffalo Wild Wings': '17420499',
              "New Delhi Diamond's Restaurant": '17420007', 'D. P. Dough': '17419924', "Joe's Restaurant": '17419965',
              'Mercato Bar & Kitchen': '17420530', "Rulloff's Restaurant": '17420032', 'Asia Cuisine': '17419881',
              'Coltivare': '17420730', 'Sumo Japanese Restaurant': '17420600', 'Cornell Dairy Bar': '17420414',
              'Gorgers': '17420598', 'Mia': '17420569', 'Wegmans Food Pharmacy': '17420081',
              'Hai Hong Restaurant': '17419951', "Sweet Melissa's Ice Cream Shop": '17420485',
              "Red's Place": '17420691', 'Sahara': '17420528', 'Plum Tree Japanese Restaurant': '17420021',
              'Vietnam Restaurant': '17420076', 'Ithaca Beer Company': '17420640', 'Wings Over Ithaca': '17420085',
              "Banfi's Restaurant": '17419884', 'Napoli Pizzeria': '17420004', 'Dolce Delight': '17420424',
              'Tokyo Hibachi': '17420614', 'Pizza Aroma': '17420019', 'Capital Corner': '17419902',
              'Thai Basil Restaurant': '17420595', 'Four Seasons': '17419937', 'Northeast Pizza': '17420537',
              'Sangam Indian Cuisine': '17420037', "Ned's Pizza": '17420006', 'Asian Noodle House': '17419882',
              'Istanbul Turkish Kitchen': '17420715', "Sammy's Pizzeria & Restaurant": '17420035',
              "Kelly's Dock-Side Cafe": '17419974', 'Casablanca Pizzeria & Med Cuisine': '17420571',
              'Hot Truck': '17419955', "Friendly's": '17419940', 'Apollo Restaurant': '17419877',
              "Papa John's Pizza": '17420012', "Moe's Southwest Grill": '17420469', 'Stellas Barn': '17420443',
              'Hawi Ethiopian': '17795583', 'College Town Pizza': '17419910', 'Ling Ling Garden': '17419982',
              'Miyake Japanese Restaurant': '17420001', 'Samurai': '17420036', "Chili's": '17420411',
              'The Shop Cafe': '17420645', 'Italian Carry Out': '17419958', "Louie's Lunch": '17419987',
              'Pokeland': '17866936', 'Rose Restaurant': '17420029', "Jimmy John's": '17420481',
              'Ithaca Coffee Company': '17420430', "Applebee's": '17419878', 'Jade Garden Restaurants': '17419963',
              "Jason's Grocery & Deli": '17419964', 'Starbucks': '17420051', 'Le Café Cent-dix': '17420729',
              'Little Thai House': '17419984', 'The Rook': '17866933', "UNCLE Joe's Sports Bar": '17420075',
              "Rogan's Corner Northeast": '17420028', "Simeon's American Bistro": '18395843',
              'Gola Osteria': '17420727', "McDonald's": '17419998', 'Koko Korean Restauurant': '17419978',
              'The Old Tea House': '17420502', "Domino's Pizza": '17419930', 'Cafe Pacific': '17419901',
              'Ling Ling Restaurant': '17419983', "Sarah's Patisserie": '17420039', 'Ithaca To Go': '17420557',
              'Lincoln Street Diner': '17420588', 'Greenstar Deli': '17420615', 'Sicilian Delight Pizzeria': '17420043',
              'Sunset Grill': '17420603', 'Spicy Asian': '17420722', "Pete's Grocery": '17420431',
              'Pizza Hut': '17420020', 'Simply Red Bistro at La Tourelle Resort': '17420599',
              "Sal's Pizzeria": '17420034', "Pudgie's Pizza & Sub Shops": '17420022', 'Firehouse Subs': '17420642',
              "Fat Jack's BBQ": '17420664', 'Coal Yard Cafe': '17420638', "Jack's Grill": '17420586',
              'Chipotle Mexican Grill': '17795536', 'Luna Inspired Street Food': '18012528', 'Burger King': '17419896',
              "J J's Cafe": '17419962', 'Ithaca Diner': '17419961', 'Just Desserts': '17419969', 'Subway': '17420057',
              'Insomnia Cookies': '17420579', 'The Rose': '17420646', 'Collegetown Crepes': '17420724',
              'Establishment': '17420639', 'Old Mexico': '17866923', 'Texas Roadhouse': '17839802',
              'Fall Creek House Restaurant': '17419935', 'Second Landing': '17420041', 'Taco Bell': '17420065',
              'Ithaca Coffee Company': '17420429', 'Chinese Buffet Restaurant': '17419908', 'Oishii Bowl': '17757737',
              'Easy Wok': '17419934', "Dunbar's": '17419932', "Pete's Cayuga Bar": '17420016', 'The Haunt': '17420072',
              "Big Al's Hilltop Quikstop INC": '17419889', 'The Plantations Bar and Grill': '17420551',
              'De Tasty Hot Pot': '17420723', "Arby's": '17419879', 'Lot 10 Kitchen': '17420587',
              "Rogan's Corner": '17757735', 'Tim Hortons': '17420491', 'Gorgers Taco Shack': '17420716',
              'Mitsuba Hibachi Sushi Restaurant': '17795561', 'Due Amici': '18433192'}
    fullDataDict = {}
    with open('ZomatoData.txt', 'rb') as f:
        data = f.readlines()
    for partial_data in data:
        newData = json.loads(partial_data)
        fullDataDict.update(newData)
    for elem in fullDataDict:
        key = fullDataDict[elem][1]
        id = idDict.get(key)
        fullDataDict[elem].append(id)
    print(fullDataDict)
    with open('ZomatoData2.txt', 'a+') as outfile:
        json.dump(fullDataDict, outfile)


addRestaurantIDs()

