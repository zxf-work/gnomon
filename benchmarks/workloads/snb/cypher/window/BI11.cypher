// BI11, window setting (rule W1): one co-valid triangle binding (three persons with their place chains, three
// friendships in the period), counted per version tuple as the SQL text does. Each person's place chain and each
// friendship is its own MATCH clause: within one clause Cypher binds a relationship once, so two persons in one city
// would lose the shared IS_PART_OF (the corpus row's P6 issue); SQL joins the same row twice.
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
