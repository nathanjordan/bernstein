from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
import urlparse
from tld import get_tld
import csv

from scrapy.item import Item
from scrapy.item import Field
from scrapy.spider import Spider
#import scrapy.contrib.spiders.CrawlSpider as CrawlSpider
#import scrapy.contrib.spiders.Rule as Rule
from scrapy.selector import Selector

import database

urls = []
domains = {}
disallowed_filetypes = ['.jpg', '.gif', '.png', '.svg', '.tiff', '.pdf']


def is_absolute(url):
    return bool(urlparse.urlparse(url).netloc)


# Read the .csv
with open('sites_minus_facebook.csv', 'r') as csvfile:
    url_reader = csv.reader(csvfile)
    # For each row, add the url and get the domain
    for row in url_reader:
        urls.append(row)
        domains[get_tld(row[3])] = True


class Link(Item):

    url = Field()
    label = Field()


class TestSpider(Spider):

    name = "bernstein_spider"
    allowed_domains = domains
    start_urls = [x[3] for x in urls]

    def parse(self, response):
        sel = Selector(response)
        # for each anchor that has a href attribute
        for url in sel.css("a::attr(href)").extract():
            # if the url is relative, make it absolute
            if not is_absolute(url):
                url = urlparse.urljoin(response.url, url.strip())
            # parse the url
            o = urlparse.urlparse(url)
            # strip the queries
            queryless_url = o.scheme + "://" + o.netloc + o.path
            # normalize the url
            queryless_url = queryless_url.lower()
            #get the tld
            url_tld = get_tld(queryless_url, fail_silently=True)
            # check to see if its in the allowed domains before we add it
            if url_tld not in domains:
                continue
            # make sure its not a javascript link?
            #if 'javascript' in queryless_url:
            #    continue
            # make sure the filetype is right
            filetype_allowed = True
            for filetype in disallowed_filetypes:
                if filetype in queryless_url:
                    filetype_allowed = False
                    break
            if not filetype_allowed:
                continue
            # add it to the database
            database.map_link(self,
                              response.request.url.lower(),
                              queryless_url)
            # crawl the resulting link
            yield Request(queryless_url, callback=self.parse)


if __name__ == "__main__":
    # create initial nodes
    for url in urls:
        database.create_initial_node(url[3].lower())
    # create a new spider
    spider = TestSpider()
    # set the crawler settings
    proj_settings = get_project_settings()
    crawler = Crawler(proj_settings)
    # Set custom crawler settings
    crawler.settings.overrides['DEPTH_LIMIT'] = 7
    crawler.settings.overrides['CONCURRENT_REQUESTS'] = 32
    crawler.settings.overrides['DOWNLOAD_DELAY'] = 0.1
    # tell twisted we want to stop when the spider is done
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    # configure and set which spider the crawler is using
    crawler.configure()
    crawler.crawl(spider)
    # start crawling
    crawler.start()
    # log to a file instead of stdout
    log.start(loglevel=log.DEBUG, logfile="bernstein.log", logstdout=True)
    # start the twisted event loop
    reactor.run()
