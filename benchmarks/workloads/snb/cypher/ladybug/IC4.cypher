// IC4 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC4.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R3.
MATCH (person:Person {id: $personId })-[k:KNOWS]-(friend:Person),
      (friend)<-[h:HAS_CREATOR]-(post:Post)-[ht:HAS_TAG]->(tag)
WHERE VIS(person, k, friend, h, post, ht, tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN post.creationDate < $endDate AND post.creationDate >= $startDate THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN $startDate > post.creationDate THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10
