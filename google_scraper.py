import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os
from selenium.webdriver.common.by import By

import time
def scrapeGoogle(url):

    cwd = os.getcwd()
    driverPath = cwd + '/chromedriver'
    driver = webdriver.Chrome(executable_path= driverPath)
    driver.get(url)
    time.sleep(5)
    for i in range(5):
        elem = driver.find_element_by_xpath(
            "//div[@class ='section-listbox section-scrollbox scrollable-y scrollable-show']")
        elem.send_keys(Keys.END)
        time.sleep(0.5)


    soup = BeautifulSoup(driver.page_source, 'lxml')
    for reviewFull in soup.find_all('div', class_='section-review ripple-container'):
        print(reviewFull.findChild('span',class_='section-review-text').text)
        print('')



scrapeGoogle('https://www.google.com/maps/place/Taverna+Banfi/@42.4464947,-76.4844149,17z/data=!3m1!4b1!4m10!1m2!2m1!1srestaurants+in+ithaca!3m6!1s0x89d0818b0848a40f:0xbabc00374d300924!8m2!3d42.4464908!4d-76.4822262!9m1!1b1')
