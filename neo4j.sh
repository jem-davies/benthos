docker run \
    --restart always \
    --publish=7474:7474 --publish=7687:7687 \
    --env NEO4J_AUTH=none \
    neo4j:5.20.0

## Return all nodes and relations: 
# MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN n, r

## Delete all nodes and relations
# MATCH (n) DETACH DELETE n