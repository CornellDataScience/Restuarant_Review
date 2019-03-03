import json
from multiprocessing import Process

def scrape_for_url(url):
    print('start scrapping for url' + url)
    # scrape_data = scrapeYelp(url)
    # print(scrape_data)
    # with open('YelpData.txt', 'a+') as outfile:
    #     json.dump(scrape_data, outfile)
    #     outfile.write("\n")


# use yelp api to scrape Yelp data simultaneously
def main():
    processes = []

    # name_url_dict = getRestaurants('Ithaca')
    #
    # # create new YelpData.txt file
    # open('YelpData.txt', 'w').close()
    #
    # # create new processes for all urls and start scrape at the same time
    # for elem in name_url_dict:
    #     p = Process(target=scrape_for_url, args=(name_url_dict[elem],))
    #     p.start()
    #     processes.append(p)
    #
    # for p in processes:
    #     p.join()


main()
