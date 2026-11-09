// BI8, window setting (rule W1): the interest and message scores are stages over their own valid bindings; the
// friends' sum joins the scores on the friendship's key.
MATCH (tag:Tag {name: $tag})
OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person:Person)
WHERE W_OK(tag, interest, person)
WITH tag, collect(person) AS interestedPersons
OPTIONAL MATCH (tag)<-[ht:HAS_TAG]-(message:Message)-[hc:HAS_CREATOR]->(person:Person)
WHERE $startDate < message.creationDate AND message.creationDate < $endDate AND W_OK(tag, ht, message, hc, person)
WITH tag, interestedPersons, collect(person) AS messagePersons
WITH tag, interestedPersons + messagePersons AS persons
UNWIND persons AS person
WITH DISTINCT tag, person.id AS pid
CALL {
  WITH tag, pid
  OPTIONAL MATCH (tag)<-[i1:HAS_INTEREST]-(p:Person {id: pid})
  WHERE W_OK(tag, i1, p)
  WITH tag, pid, count(DISTINCT p) AS ni
  OPTIONAL MATCH (tag)<-[t1:HAS_TAG]-(m1:Message)-[c1:HAS_CREATOR]->(q:Person {id: pid})
  WHERE $startDate < m1.creationDate AND m1.creationDate < $endDate AND W_OK(tag, t1, m1, c1, q)
  WITH ni, count(m1) AS nm
  RETURN CASE WHEN ni > 0 THEN 100 ELSE 0 END + nm AS score
}
WITH collect({pid: pid, score: score}) AS scores
UNWIND scores AS s
OPTIONAL MATCH (:Person {id: s.pid})-[k:KNOWS]-(f:Person)
WHERE W_OK(k)
WITH s, scores, collect(f.id) AS friends
RETURN s.pid AS `person.id`, s.score AS score,
       reduce(acc = 0, fid IN friends | acc + coalesce(head([y IN scores WHERE y.pid = fid | y.score]), 0)) AS friendsScore
ORDER BY score + friendsScore DESC, `person.id` ASC
LIMIT 100
