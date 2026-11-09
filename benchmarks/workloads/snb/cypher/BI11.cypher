// BI11. Deviation: + VIS on every variable (the anonymous places are named).
MATCH (a:Person)-[ila:IS_LOCATED_IN]->(ca:City)-[ipa:IS_PART_OF]->(country:Country {name: $country}),
      (a)-[k1:KNOWS]-(b:Person)
WHERE a.id < b.id
  AND $startDate <= k1.creationDate AND k1.creationDate <= $endDate AND VIS(a, ila, ca, ipa, country, k1, b)
WITH DISTINCT country, a, b
MATCH (b)-[ilb:IS_LOCATED_IN]->(cb:City)-[ipb:IS_PART_OF]->(country)
WHERE VIS(ilb, cb, ipb)
WITH DISTINCT country, a, b
MATCH (b)-[k2:KNOWS]-(c:Person),
      (c)-[ilc:IS_LOCATED_IN]->(cc:City)-[ipc:IS_PART_OF]->(country)
WHERE b.id < c.id
  AND $startDate <= k2.creationDate AND k2.creationDate <= $endDate AND VIS(k2, c, ilc, cc, ipc)
WITH DISTINCT a, b, c
MATCH (c)-[k3:KNOWS]-(a)
WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate AND VIS(k3)
WITH DISTINCT a, b, c
RETURN count(*) AS count
