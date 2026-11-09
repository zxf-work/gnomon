// BI2 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI2.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2.
MATCH (tag:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE VIS(tag, ht, tc)
OPTIONAL MATCH (message1:Post:Comment)-[h1:HAS_TAG]->(tag)
  WHERE $date <= message1.creationDate
    AND message1.creationDate < $date + to_days(100) AND VIS(message1, h1)
WITH tag, count(message1) AS countWindow1
OPTIONAL MATCH (message2:Post:Comment)-[h2:HAS_TAG]->(tag)
  WHERE $date + to_days(100) <= message2.creationDate
    AND message2.creationDate < $date + to_days(200) AND VIS(message2, h2)
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
