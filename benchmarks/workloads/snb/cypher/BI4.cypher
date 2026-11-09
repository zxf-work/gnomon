// BI4. Deviation: + VIS on every variable (inside the CALL subqueries as well), REPLY_OF*0.. as a named path.
MATCH (country:Country)<-[ip:IS_PART_OF]-(c:City)<-[il:IS_LOCATED_IN]-(person:Person)<-[hm:HAS_MEMBER]-(forum:Forum)
WHERE forum.creationDate > $date AND VIS(country, ip, c, il, person, hm, forum)
WITH country, forum, count(person) AS numberOfMembers
ORDER BY numberOfMembers DESC, forum.id ASC, country.id
WITH DISTINCT forum AS topForum
LIMIT 100
WITH collect(topForum) AS topForums
CALL {
  WITH topForums
  UNWIND topForums AS topForum1
  MATCH (topForum1)-[cf:CONTAINER_OF]->(post:Post), w = (post)<-[:REPLY_OF*0..]-(message:Message),
        (message)-[hc:HAS_CREATOR]->(person:Person)<-[hm2:HAS_MEMBER]-(topForum2:Forum)
  WHERE VIS(cf, post, hc, person, hm2, topForum2) AND VISALL(nodes(w)) AND VISALL(relationships(w))
  WITH person, message, topForum2
  WHERE topForum2 IN topForums
  RETURN person, count(DISTINCT message) AS messageCount
UNION ALL
  WITH topForums
  UNWIND topForums AS topForum1
  MATCH (person:Person)<-[hm3:HAS_MEMBER]-(topForum1:Forum)
  WHERE VIS(person, hm3)
  RETURN person, 0 AS messageCount
}
RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  sum(messageCount) AS messageCount
ORDER BY
  messageCount DESC,
  person.id ASC
LIMIT 100
