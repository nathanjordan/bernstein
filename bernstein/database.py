from py2neo import neo4j, node, rel

db = neo4j.GraphDatabaseService("http://localhost:7474/db/bernstein")

db.create()

print db
