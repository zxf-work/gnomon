// HAND BI10 for LadybugDB (window, rule W1), from cypher/window/BI10.cypher by R1-R6 and: the walks are [lo, hi, depth]
// lists (not maps); the shorter walks of a walk are collected over a second UNWIND of the walk list with the depth
// test in a CASE (no lambda reads the depth: LB-10; the first form, a list_filter reading it, differed on 1 of 6
// instances at SF0.1, 2026-10-24) (D-W3-13).
MATCH (cand:Person)-[il:IS_LOCATED_IN]->(ci:City)-[ip:IS_PART_OF]->(co:Country {name: $country}),
      (cand)<-[hc:HAS_CREATOR]-(message:Post:Comment)-[h1:HAS_TAG]->(t1:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE cand.id <> $personId AND W_OK(cand, il, ci, ip, co, hc, message, h1, t1, ht, tc)
WITH DISTINCT cand, message, W_LOV(cand, il, ci, ip, co, hc, message, h1, t1, ht, tc) AS lo, W_HIV(cand, il, ci, ip, co, hc, message, h1, t1, ht, tc) AS hi
MATCH w = (startPerson:Person {id: $personId})-[:KNOWS* TRAIL 1..4 (_r, _n | WHERE WPRUNE(_r, _n))]-(cand)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH cand, message, lo, hi, w, W_LOV(| nodes(w) + relationships(w)) AS a, W_HIV(| nodes(w) + relationships(w)) AS b
WITH cand, message, collect([CASE WHEN a > lo THEN a ELSE lo END, CASE WHEN b < hi THEN b ELSE hi END, CAST(length(w) AS INT64)]) AS walks
UNWIND walks AS wk
WITH cand, message, walks, wk[1] AS wlo, wk[2] AS whi, wk[3] AS wd
WHERE $minPathDistance <= wd AND wd <= $maxPathDistance AND wlo < whi
UNWIND walks AS s2
WITH cand, message, wlo, whi, wd, collect(CASE WHEN s2[3] < wd THEN [s2[1], s2[2]] ELSE NULL END) AS shorter
WHERE W_UNCOVERED(wlo, whi, shorter)
WITH DISTINCT cand, message
MATCH (message)-[h2:HAS_TAG]->(tag:Tag)
WHERE W_OK(h2, tag)
RETURN cand.id AS `cand.id`, tag.name AS `tag.name`, count(*) AS messageCount
ORDER BY messageCount DESC, `tag.name` ASC, `cand.id` ASC
LIMIT 100
