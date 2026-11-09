// BI10, window setting (rule W1): at some instant of the (person, place chain, message, tag edges, tag class) binding
// the least length of a co-valid KNOWS walk is in [min, max] (per instant, G11 verdict 2): some walk of a length in
// the band has an instant of its interval that no shorter walk covers. The per-tag count joins the message's tags as
// a stage.
MATCH (cand:Person)-[il:IS_LOCATED_IN]->(ci:City)-[ip:IS_PART_OF]->(co:Country {name: $country}),
      (cand)<-[hc:HAS_CREATOR]-(message:Message)-[h1:HAS_TAG]->(t1:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE cand.id <> $personId AND W_OK(cand, il, ci, ip, co, hc, message, h1, t1, ht, tc)
WITH DISTINCT cand, message, W_LOV(cand, il, ci, ip, co, hc, message, h1, t1, ht, tc) AS lo, W_HIV(cand, il, ci, ip, co, hc, message, h1, t1, ht, tc) AS hi
MATCH w = (startPerson:Person {id: $personId})-[:KNOWS*1..4]-(cand)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH cand, message, lo, hi, w, W_LOV(| nodes(w) + relationships(w)) AS a, W_HIV(| nodes(w) + relationships(w)) AS b
WITH cand, message, collect({lo: CASE WHEN a > lo THEN a ELSE lo END, hi: CASE WHEN b < hi THEN b ELSE hi END, d: length(w)}) AS walks
UNWIND walks AS wk
WITH cand, message, wk, [s IN walks WHERE s.d < wk.d | [s.lo, s.hi]] AS shorter
WHERE $minPathDistance <= wk.d AND wk.d <= $maxPathDistance AND wk.lo < wk.hi AND W_UNCOVERED(wk.lo, wk.hi, shorter)
WITH DISTINCT cand, message
MATCH (message)-[h2:HAS_TAG]->(tag:Tag)
WHERE W_OK(h2, tag)
RETURN cand.id, tag.name, count(*) AS messageCount
ORDER BY messageCount DESC, tag.name ASC, cand.id ASC
LIMIT 100
