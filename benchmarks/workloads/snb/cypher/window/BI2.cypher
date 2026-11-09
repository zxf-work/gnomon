// BI2, window setting (rule W1): the tag list is a stage; each counted message is a co-valid (tag class, type edge,
// tag, tag edge, message) binding.
MATCH (tag:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE W_OK(tag, ht, tc)
WITH DISTINCT tag
OPTIONAL MATCH (message1:Message)-[h1:HAS_TAG]->(tag)-[ht1:HAS_TYPE]->(tc1:TagClass {name: $tagClass})
  WHERE $date <= message1.creationDate AND message1.creationDate < $date + duration({days: 100}) AND W_OK(message1, h1, tag, ht1, tc1)
WITH tag, count(message1) AS countWindow1
OPTIONAL MATCH (message2:Message)-[h2:HAS_TAG]->(tag)-[ht2:HAS_TYPE]->(tc2:TagClass {name: $tagClass})
  WHERE $date + duration({days: 100}) <= message2.creationDate AND message2.creationDate < $date + duration({days: 200})
    AND W_OK(message2, h2, tag, ht2, tc2)
WITH tag, countWindow1, count(message2) AS countWindow2
RETURN tag.name, countWindow1, countWindow2, abs(countWindow1 - countWindow2) AS diff
ORDER BY diff DESC, tag.name ASC
LIMIT 100
