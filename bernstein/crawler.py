from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
import urlparse
import csv

from scrapy.item import Item
from scrapy.item import Field
from scrapy.spider import Spider
#import scrapy.contrib.spiders.CrawlSpider as CrawlSpider
#import scrapy.contrib.spiders.Rule as Rule
from scrapy.selector import Selector

import database

urls = []
domains = []

# Read the .csv
with open('sites.csv', 'r') as csvfile:
    url_reader = csv.reader(csvfile)
    # For each row, add the url and get the domain
    for row in url_reader:
        urls.append(row)
        o = urlparse.urlparse(row[3])
        domains.append(o.netloc)


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
            # parse the url
            # TODO Might need to change something for absolute urls here
            abs_url = urlparse.urljoin(response.url, url.strip())
            o = urlparse.urlparse(abs_url)
            # strip the queries
            queryless_url = o.scheme + "://" + o.netloc + o.path
            # get the domain info to check if we're crawling something we don't
            # need
            netloc = o.netloc.lower()
            # normalize the url
            queryless_url = queryless_url.lower()
            # make sure its not a javascript link?
            if 'javascript' in queryless_url:
                continue
            # check to see if its in the allowed domains before we add it
            allowed = False
            for domain in self.allowed_domains:
                # if the domain is in the netloc, we can add it
                if domain in netloc:
                    allowed = True
                    break
            # otherwise move on to the next link
            if not allowed:
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
        database.create_initial_node(url[3])
    # create a new spider
    spider = TestSpider()
    # set the crawler settings
    settings = get_project_settings()
    crawler = Crawler(settings)
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
