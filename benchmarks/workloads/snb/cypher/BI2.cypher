// BI2. Deviation: + VIS on every variable.
MATCH (tag:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE VIS(tag, ht, tc)
OPTIONAL MATCH (message1:Message)-[h1:HAS_TAG]->(tag)
  WHERE $date <= message1.creationDate
    AND message1.creationDate < $date + duration({days: 100}) AND VIS(message1, h1)
WITH tag, count(message1) AS countWindow1
OPTIONAL MATCH (message2:Message)-[h2:HAS_TAG]->(tag)
  WHERE $date + duration({days: 100}) <= message2.creationDate
    AND message2.creationDate < $date + duration({days: 200}) AND VIS(message2, h2)
WITH
  tag,
  countWindow1,
  count(message2) AS countWindow2
RETURN
  tag.name,
  countWindow1,
  countWindow2,
  abs(countWindow1 - countWindow2) AS diff
ORDER BY
  diff DESC,
  tag.name ASC
LIMIT 100
