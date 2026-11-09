// BI16, window setting (rule W1): per tag/day, the person stage over valid (person, creator edge, message, tag edge,
// tag) bindings; the induced friendships are joined on the stage's person keys.
UNWIND [{letter: 'A', tag: $tagA, date: $dateA}, {letter: 'B', tag: $tagB, date: $dateB}] AS param
WITH param.letter AS paramLetter, param.tag AS paramTagX, param.date AS paramDateX
CALL {
  WITH paramTagX, paramDateX
  MATCH (person1:Person)<-[h1:HAS_CREATOR]-(message1:Message)-[t1:HAS_TAG]->(tag:Tag {name: paramTagX})
  WHERE date(message1.creationDate) = date(paramDateX) AND W_OK(person1, h1, message1, t1, tag)
  WITH paramTagX, paramDateX, collect(DISTINCT person1.id) AS sg, collect({p: person1.id, m: message1.id}) AS pm
  UNWIND pm AS x
  WITH sg, x.p AS pid, count(DISTINCT x.m) AS cm
  OPTIONAL MATCH (:Person {id: pid})-[k:KNOWS]-(p2:Person)
  WHERE p2.id IN sg AND W_OK(k)
  WITH pid, cm, count(DISTINCT p2.id) AS cp2
  WHERE cp2 <= $maxKnowsLimit
  RETURN pid, cm
}
WITH pid, collect({letter: paramLetter, messageCount: cm}) AS results
WHERE size(results) = 2
RETURN pid AS `person1.id`,
  [r IN results WHERE r.letter = 'A' | r.messageCount][0] AS messageCountA,
  [r IN results WHERE r.letter = 'B' | r.messageCount][0] AS messageCountB
ORDER BY messageCountA + messageCountB DESC, `person1.id` ASC
LIMIT 20
