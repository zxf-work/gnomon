// BI6. Deviation: + VIS on every variable.
MATCH (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message1:Message)-[hc:HAS_CREATOR]->(person1:Person)
WHERE VIS(tag, ht, message1, hc, person1)
OPTIONAL MATCH (message1)<-[l1:LIKES]-(person2:Person)
WHERE VIS(l1, person2)
OPTIONAL MATCH (person2)<-[h2:HAS_CREATOR]-(message2:Message)<-[like:LIKES]-(person3:Person)
WHERE VIS(h2, message2, like, person3)
RETURN
  person1.id,
  count(DISTINCT like) AS authorityScore
ORDER BY
  authorityScore DESC,
  person1.id ASC
LIMIT 100
