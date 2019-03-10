import json
from multiprocessing import Process
from Zomato_Web_Scraper import scrapeZomato

def scrape_for_url(url):

    print('start scrapping for url ' + url)
    scrape_data = scrapeZomato(url)
    with open('ZomatoData.txt', 'a+') as outfile:
        json.dump(scrape_data, outfile)
        outfile.write("\n")


# use yelp api to scrape Zomato data simultaneously
def main():
    processes = []

    # create new ZomatoData.txt file
    open('ZomatoData.txt', 'w').close()

    # create new processes for all urls and start scrape at the same time
    filename = "ZomatoURLs"
    url_file = open(filename, "r")

    # i = 0
    # for url in url_file:
    #     if i < 5:
    #         p = Process(target=scrape_for_url, args=(url,))
    #         p.start()
    #         processes.append(p)
    #         i += 1
    #
    # for p in processes:
    #     p.join()

    for url in url_file:
        scrape_for_url(url)


main()

# restaurant id and 5 review ids
