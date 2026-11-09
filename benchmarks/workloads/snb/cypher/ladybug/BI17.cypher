// BI17 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI17.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH
  (tag:Tag {name: $tag}),
  (person1:Person)<-[c1:HAS_CREATOR]-(message1:Post:Comment), w1 = (message1)-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]->(post1:Post),
  (post1)<-[f1:CONTAINER_OF]-(forum1:Forum),
  (message1)-[t1:HAS_TAG]->(tag),
  (forum1)-[m2:HAS_MEMBER]->(person2:Person)<-[c2:HAS_CREATOR]-(comment:Comment)-[t2:HAS_TAG]->(tag),
  (forum1)-[m3:HAS_MEMBER]->(person3:Person)<-[c3:HAS_CREATOR]-(message2:Post:Comment),
  (comment)-[r:REPLY_OF]->(message2), w2 = (message2)-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]->(post2:Post),
  (post2)<-[f2:CONTAINER_OF]-(forum2:Forum),
  (message2)-[t3:HAS_TAG]->(tag)
WHERE forum1 <> forum2
  AND message2.creationDate > message1.creationDate + to_hours($delta)
  AND VIS(tag, person1, c1, message1, post1, f1, forum1, t1, m2, person2, c2, comment, t2, m3, person3, c3, message2, r, post2, f2, forum2, t3)
  AND VIS(message1, post1) AND VIS(message2, post2)
OPTIONAL MATCH (forum2)-[mx:HAS_MEMBER]->(person1)
WHERE VIS(mx)
WITH person1, message2, count(mx) AS member
WHERE member = 0
RETURN person1.id, count(DISTINCT message2) AS messageCount
ORDER BY messageCount DESC, person1.id ASC
LIMIT 10
