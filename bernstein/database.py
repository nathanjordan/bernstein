from py2neo import neo4j
from scrapy import log
import urlparse

# connect to the db
db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

# create the url node index
url_index = db.get_or_create_index(neo4j.Node, "url_index")


def create_initial_node(url):
    # set the node properties (currently just the url)
    initial_node_props = {"url": urlparse.urlparse(url).netloc,
                          "word_list": [u""]}
    # add the node to the index
    db.get_or_create_indexed_node("url_index",
                                  "url",
                                  url,
                                  initial_node_props)


def map_link(spider, url_referrer, url_destination):
    # create properties for the destination node
    destination_props = {"url": url_destination, "word_list": [u""]}
    # add the node to the index
    destination_node = db.get_or_create_indexed_node("url_index",
                                                     "url",
                                                     url_destination,
                                                     destination_props)
    # get the node that referred to this one (it should definitely exist)
    referrer_node = db.get_indexed_node("url_index", "url", url_referrer)
    # if the referrer node doesn't exist, we were probably 302ed
    # TODO is this the best option?
    if not referrer_node:
        return
    # get links between these pages
    links = destination_node.match_incoming(start_node=referrer_node,
                                            rel_type="LINKS_TO")
    link_list = list(links)
    # if theres not already a link, create one
    if not len(link_list):
        referrer_node.create_path("LINKS_TO", destination_node)


def add_corpus(spider, url_host, url_referrer, word_list):
    referrer_node = db.get_indexed_node("url_index", "url", url_host)
    # TODO is this the best option?
    if not referrer_node:
        spider.log("Couldn't add corpus to referenced URL " + url_host,
                   level=log.WARNING)
        return
    node_word_list = referrer_node['word_list']
    node_word_list += word_list
    # spider.log(str([type(x) for x in node_word_list]), level=log.WARNING)
    referrer_node.update_properties({'word_list': node_word_list})

def get_all_nodes():
    nodes = db.get_or_create_index(neo4j.Node, 'url_index')
    return nodes.query("url:*")

def initialize():
    # create the database if its not already there
    db.create()
    # clear the database
    db.clear()
