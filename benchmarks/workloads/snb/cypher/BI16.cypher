// BI16. Deviation: + VIS on every variable (inside the CALL subquery).
UNWIND [
    {letter: 'A', tag: $tagA, date: $dateA},
    {letter: 'B', tag: $tagB, date: $dateB}
  ] AS param
WITH param.letter AS paramLetter, param.tag AS paramTagX, param.date AS paramDateX
CALL {
  WITH paramTagX, paramDateX
  MATCH (person1:Person)<-[h1:HAS_CREATOR]-(message1:Message)-[t1:HAS_TAG]->(tag:Tag {name: paramTagX})
  WHERE date(message1.creationDate) = date(paramDateX) AND VIS(person1, h1, message1, t1, tag)
  OPTIONAL MATCH (person1)-[k:KNOWS]-(person2:Person)<-[h2:HAS_CREATOR]-(message2:Message)-[t2:HAS_TAG]->(tag)
  WHERE date(message2.creationDate) = date(paramDateX) AND VIS(k, person2, h2, message2, t2)
  WITH person1, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2
  WHERE cp2 <= $maxKnowsLimit
  RETURN person1, cm
}
WITH person1, collect({letter: paramLetter, messageCount: cm}) AS results
WHERE size(results) = 2
RETURN
  person1.id,
  [r IN results WHERE r.letter = 'A' | r.messageCount][0] AS messageCountA,
  [r IN results WHERE r.letter = 'B' | r.messageCount][0] AS messageCountB
ORDER BY messageCountA + messageCountB DESC, person1.id ASC
LIMIT 20
