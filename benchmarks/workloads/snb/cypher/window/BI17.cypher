// BI17, window setting (rule W1): one co-valid binding (both REPLY_OF*0.. walks included); "person1 is not a member of
// forum2" is W1's NOT (W_UNCOVERED).
MATCH
  (tag:Tag {name: $tag}),
  (person1:Person)<-[c1:HAS_CREATOR]-(message1:Message), w1 = (message1)-[:REPLY_OF*0..]->(post1:Post),
  (post1)<-[f1:CONTAINER_OF]-(forum1:Forum),
  (message1)-[t1:HAS_TAG]->(tag),
  (forum1)-[m2:HAS_MEMBER]->(person2:Person)<-[c2:HAS_CREATOR]-(comment:Comment)-[t2:HAS_TAG]->(tag),
  (forum1)-[m3:HAS_MEMBER]->(person3:Person)<-[c3:HAS_CREATOR]-(message2:Message),
  (comment)-[r:REPLY_OF]->(message2), w2 = (message2)-[:REPLY_OF*0..]->(post2:Post),
  (post2)<-[f2:CONTAINER_OF]-(forum2:Forum),
  (message2)-[t3:HAS_TAG]->(tag)
WHERE forum1 <> forum2 AND person2.id <> person3.id
  AND message2.creationDate > message1.creationDate + duration({hours: $delta})
  AND W_OK(tag, person1, c1, message1, post1, f1, forum1, t1, m2, person2, c2, comment, t2, m3, person3, c3, message2, r, post2, f2, forum2, t3 | nodes(w1) + relationships(w1) + nodes(w2) + relationships(w2))
WITH person1, message2, forum2,
     W_LOV(tag, person1, c1, message1, post1, f1, forum1, t1, m2, person2, c2, comment, t2, m3, person3, c3, message2, r, post2, f2, forum2, t3 | nodes(w1) + relationships(w1) + nodes(w2) + relationships(w2)) AS lo,
     W_HIV(tag, person1, c1, message1, post1, f1, forum1, t1, m2, person2, c2, comment, t2, m3, person3, c3, message2, r, post2, f2, forum2, t3 | nodes(w1) + relationships(w1) + nodes(w2) + relationships(w2)) AS hi
OPTIONAL MATCH (forum2)-[mx:HAS_MEMBER]->(person1)
WHERE W_INV(lo, hi, mx)
WITH person1, message2, lo, hi, collect(CASE WHEN mx IS NULL THEN null ELSE W_EXT(mx) END) AS ext
WHERE W_UNCOVERED(lo, hi, ext)
RETURN person1.id, count(DISTINCT message2) AS messageCount
ORDER BY messageCount DESC, person1.id ASC
LIMIT 10
