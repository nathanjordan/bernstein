from py2neo import neo4j

db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

db.create()

db.clear()

url_index = db.get_or_create_index(neo4j.Node, "url_index")


def create_initial_node(url):
    initial_node_props = {"url": url}
    db.get_or_create_indexed_node("url_index",
                                  "url",
                                  url,
                                  initial_node_props)


def map_link(spider, url_referrer, url_destination):
    destination_props = {"url": url_destination}
    destination_node = db.get_or_create_indexed_node("url_index",
                                                     "url",
                                                     url_destination,
                                                     destination_props)
    referrer_node = db.get_indexed_node("url_index", "url", url_referrer)
    links = destination_node.match_incoming(start_node=referrer_node,
                                            rel_type="LINKS_TO")
    link_list = list(links)
    if not len(link_list):
        referrer_node.create_path("LINKS_TO", destination_node)
