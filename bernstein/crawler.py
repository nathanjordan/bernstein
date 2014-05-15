from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
import urlparse
from tld import get_tld
import csv
import nltk

from scrapy.item import Item
from scrapy.item import Field
from scrapy.spider import Spider
from scrapy.selector import Selector

import database

urls = []
domains = {}
disallowed_filetypes = ['.jpg', '.JPG', '.jpeg', '.gif', '.png', '.svg',
                        '.tiff', '.pdf']
crawled_urls = {}


def is_absolute(url):
    return bool(urlparse.urlparse(url).netloc)


# Read the .csv
with open('sites_minus_facebook.csv', 'r') as csvfile:
    url_reader = csv.reader(csvfile)
    # For each row, add the url and get the domain
    for row in url_reader:
        urls.append(row)
        domains[urlparse.urlparse(row[3]).netloc.lower()] = True


class Link(Item):

    url = Field()
    label = Field()


class ManSpider(Spider):

    name = "bernstein_spider"
    allowed_domains = domains
    start_urls = [x[3] for x in urls]

    def parse(self, response):
        sel = Selector(response)
        request_url = urlparse.urlparse(response.request.url.lower())
        queryless_request_url = request_url.scheme + "://" + \
            request_url.netloc + request_url.path
        request_host = request_url.hostname.lower()
        # get the paragraph tags and tokenize them
        word_list = []
        for paragraph in sel.css("p::text").extract():
            word_list += nltk.word_tokenize(paragraph)
        if len(word_list) > 250:
            database.add_corpus(self, request_host, queryless_request_url,
                                word_list)
        # for each anchor that has a href attribute
        for link_url in sel.css("a::attr(href)").extract():
            # if the link url is relative, make it absolute
            if not is_absolute(link_url):
                link_url = urlparse.urljoin(response.url, link_url.strip())
            # parse the url
            link_url = urlparse.urlparse(link_url)
            # remove queries
            queryless_link_url = link_url.scheme + "://" + link_url.netloc + \
                link_url.path
            # get only domain part
            link_host = link_url.netloc.lower()
            # get the tld of the requested url
            # link_url_tld = get_tld(queryless_link_url, fail_silently=True)
            # check to see if its in the allowed domains before we add it
            if link_host not in domains:
                continue
            # if we've already crawled that page, skip it
            if queryless_link_url in crawled_urls:
                continue
            # check if the filetype is disallowed
            filetype_allowed = True
            for filetype in disallowed_filetypes:
                if filetype in queryless_link_url:
                    filetype_allowed = False
                    break
            if not filetype_allowed:
                continue
            # add it to the database
            database.map_link(self, request_host, link_host)
            # indicate the page has been crawled
            crawled_urls[queryless_link_url] = True
            # crawl the resulting link
            yield Request(queryless_link_url, callback=self.parse)


if __name__ == "__main__":
    # initialize database
    database.initialize()
    # create initial nodes
    for url in urls:
        database.create_initial_node(url[3].lower())
    # create a new spider
    spider = ManSpider()
    # set the crawler settings
    proj_settings = get_project_settings()
    crawler = Crawler(proj_settings)
    # Set custom crawler settings
    crawler.settings.overrides['DEPTH_LIMIT'] = 5
    crawler.settings.overrides['CONCURRENT_REQUESTS'] = 32
    crawler.settings.overrides['DOWNLOAD_DELAY'] = 0.05
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
