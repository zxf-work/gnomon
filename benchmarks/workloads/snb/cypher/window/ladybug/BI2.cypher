// BI2 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/BI2.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2.
MATCH (tag:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE W_OK(tag, ht, tc)
WITH DISTINCT tag
OPTIONAL MATCH (message1:Post:Comment)-[h1:HAS_TAG]->(tag)-[ht1:HAS_TYPE]->(tc1:TagClass {name: $tagClass})
  WHERE $date <= message1.creationDate AND message1.creationDate < $date + to_days(100) AND W_OK(message1, h1, tag, ht1, tc1)
WITH tag, count(message1) AS countWindow1
OPTIONAL MATCH (message2:Post:Comment)-[h2:HAS_TAG]->(tag)-[ht2:HAS_TYPE]->(tc2:TagClass {name: $tagClass})
  WHERE $date + to_days(100) <= message2.creationDate AND message2.creationDate < $date + to_days(200)
    AND W_OK(message2, h2, tag, ht2, tc2)
WITH tag, countWindow1, count(message2) AS countWindow2
RETURN tag.name, countWindow1, countWindow2, abs(countWindow1 - countWindow2) AS diff
ORDER BY diff DESC, tag.name ASC
LIMIT 100
