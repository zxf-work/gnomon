// BI11 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/BI11.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): none.
MATCH (a:Person)-[ila:IS_LOCATED_IN]->(ca:City)-[ipa:IS_PART_OF]->(na:Country {name: $country})
MATCH (a)-[k1:KNOWS]-(b:Person)
WHERE a.id < b.id AND $startDate <= k1.creationDate AND k1.creationDate <= $endDate
MATCH (b)-[ilb:IS_LOCATED_IN]->(cb:City)-[ipb:IS_PART_OF]->(nb:Country {name: $country})
MATCH (b)-[k2:KNOWS]-(c:Person)
WHERE b.id < c.id AND $startDate <= k2.creationDate AND k2.creationDate <= $endDate
MATCH (c)-[ilc:IS_LOCATED_IN]->(cc:City)-[ipc:IS_PART_OF]->(nc:Country {name: $country})
MATCH (c)-[k3:KNOWS]-(a)
WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate
  AND W_OK(a, ila, ca, ipa, na, b, ilb, cb, ipb, nb, c, ilc, cc, ipc, nc, k1, k2, k3)
RETURN count(*) AS count
