// BI6, window setting (rule W1): (tag, tag edge, message1, creator edge, person1) bindings with the liker as the
// optional part (co-valid); the liker's popularity is a stage over valid (creator edge, message2, like) bindings.
MATCH (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message1:Message)-[hc:HAS_CREATOR]->(person1:Person)
WHERE W_OK(tag, ht, message1, hc, person1)
WITH person1, message1, W_LOV(tag, ht, message1, hc, person1) AS lo, W_HIV(tag, ht, message1, hc, person1) AS hi
OPTIONAL MATCH (message1)<-[l1:LIKES]-(person2:Person)
WHERE W_INV(lo, hi, l1)
WITH DISTINCT person1, person2
OPTIONAL MATCH (person2)<-[h2:HAS_CREATOR]-(message2:Message)<-[like:LIKES]-(person3:Person)
WHERE W_OK(h2, message2, like)
WITH person1, person2, count(like) AS popularity
RETURN person1.id, sum(popularity) AS authorityScore
ORDER BY authorityScore DESC, person1.id ASC
LIMIT 100
