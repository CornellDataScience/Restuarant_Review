import json
from GetRestaurantsYelpAPI import getRestaurants
from yelp_scraper import scrapeYelp
from multiprocessing import Process

def scrape_for_url(url,id):
    print('start scrapping for url ' + url)
    scrape_data = scrapeYelp(url,id)
    print(scrape_data)
    with open('YelpData.txt', 'a+') as outfile:
        json.dump(scrape_data, outfile)
        outfile.write("\n")


# use yelp api to scrape Yelp data simultaneously
def main():
    processes = []

    name_url_dict = getRestaurants('Ithaca')

    # create new YelpData.txt file
    open('YelpData.txt', 'w').close()

    # create new processes for all urls and start scrape at the same time
    for elem in name_url_dict:
        p = Process(target=scrape_for_url, args=(name_url_dict[elem][0],name_url_dict[elem][1]))
        try:
            p.start()
        except:
            print('didnt work')

        processes.append(p)

    for p in processes:
        p.join()


main()
