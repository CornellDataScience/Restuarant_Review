import json
from multiprocessing import Process
from Zomato_Web_Scraper import scrapeZomato

def scrape_for_url(url):
    print('start scrapping for url ' + url)
    scrape_data = scrapeZomato(url)
    print(scrape_data)
    with open('ZomatoData.txt', 'a+') as outfile:
        json.dump(scrape_data, outfile)
        outfile.write("\n")


# use yelp api to scrape Zomato data simultaneously
def main():
    processes = []

    filename = "ZomatoURLs"

    # create new YelpData.txt file
    open('ZomatoData.txt', 'w').close()

    # create new processes for all urls and start scrape at the same time
    url_file = open(filename, "r")
    for url in url_file:
        p = Process(target=scrape_for_url, args=(url,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


main()
