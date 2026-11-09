// BI10. Deviation: APOC's subgraphNodes (the band of shortest-path distances [min, max]) is a shortestPath with VIS
// on every element; + VIS on every variable.
MATCH (startPerson:Person {id: $personId}), (cand:Person)-[il:IS_LOCATED_IN]->(ci:City)-[ip:IS_PART_OF]->(co:Country {name: $country})
WHERE cand.id <> $personId AND VIS(startPerson, cand, il, ci, ip, co)
MATCH path = shortestPath((startPerson)-[:KNOWS*1..10]-(cand))
WHERE VISALL(nodes(path)) AND VISALL(relationships(path))
WITH cand, length(path) AS d
WHERE $minPathDistance <= d AND d <= $maxPathDistance
MATCH (cand)<-[hc:HAS_CREATOR]-(message:Message)-[h1:HAS_TAG]->(t1:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE VIS(hc, message, h1, t1, ht, tc)
MATCH (message)-[h2:HAS_TAG]->(tag:Tag)
WHERE VIS(h2, tag)
RETURN
  cand.id,
  tag.name,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  tag.name ASC,
  cand.id ASC
LIMIT 100
