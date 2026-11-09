// IC4. Deviation: + VIS on every variable.
MATCH (person:Person {id: $personId })-[k:KNOWS]-(friend:Person),
      (friend)<-[h:HAS_CREATOR]-(post:Post)-[ht:HAS_TAG]->(tag)
WHERE VIS(person, k, friend, h, post, ht, tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN $endDate > post.creationDate >= $startDate THEN 1
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
