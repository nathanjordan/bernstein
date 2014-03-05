from py2neo import neo4j

# connect to the db
db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

# create the database if its not already there
db.create()

# clear the existing data (might remove this later)
db.clear()

# create the url node index
url_index = db.get_or_create_index(neo4j.Node, "url_index")


def create_initial_node(url):
    # set the node properties (currently just the url)
    initial_node_props = {"url": url}
    # add the node to the index
    db.get_or_create_indexed_node("url_index",
                                  "url",
                                  url,
                                  initial_node_props)


def map_link(spider, url_referrer, url_destination):
    # create properties for the destination node
    destination_props = {"url": url_destination}
    # add the node to the index
    destination_node = db.get_or_create_indexed_node("url_index",
                                                     "url",
                                                     url_destination,
                                                     destination_props)
    # get the node that referred to this one (it should definitely exist)
    referrer_node = db.get_indexed_node("url_index", "url", url_referrer)
    # get links between these pages
    links = destination_node.match_incoming(start_node=referrer_node,
                                            rel_type="LINKS_TO")
    link_list = list(links)
    # if theres not already a link, create one
    if not len(link_list):
        referrer_node.create_path("LINKS_TO", destination_node)
