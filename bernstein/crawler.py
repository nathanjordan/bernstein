from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
import urlparse

from scrapy.item import Item
from scrapy.item import Field
from scrapy.spider import Spider
#import scrapy.contrib.spiders.CrawlSpider as CrawlSpider
#import scrapy.contrib.spiders.Rule as Rule
from scrapy.selector import Selector
#import scrapy.contrib.linkextractors.sgml.SgmlLinkExtractor as SgmlLinkExtractor


class Link(Item):

    url = Field()
    label = Field()


class TestSpider(Spider):

    name = "test_spider"
    allowed_domains = ["cnn.com"]
    start_urls = ["http://cnn.com/"]

    #rules = (
    #    Rule(SgmlLinkExtractor(allow=("*", )), callback="parse_item"),
    #)

    def parse(self, response):
        sel = Selector(response)

        for url in sel.css("a::attr(href)").extract():
            abs_url = urlparse.urljoin(response.url, url.strip())
            o = urlparse.urlparse(abs_url)
            queryless_url = o.scheme + "://" + o.netloc + o.path
            self.log("Added " + queryless_url)
            yield Request(queryless_url, callback=self.parse)


if __name__ == "__main__":
    spider = TestSpider()
    settings = get_project_settings()
    crawler = Crawler(settings)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()
    log.start(loglevel=log.DEBUG, logfile="lolz.txt")
    reactor.run() # the script will block here until the spider_closed signal was sent
