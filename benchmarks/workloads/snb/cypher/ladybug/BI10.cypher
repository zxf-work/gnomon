// HAND BI10 for LadybugDB (instant settings), from cypher/BI10.cypher. Changes (D-W3-13): shortestPath(...*1..10) with
// VISALL on its elements is `* SHORTEST 1..10` with the VIS test in the step predicate, from the filtered start
// person in one MATCH (single-source; a source bound by an earlier part makes LadybugDB start from every person); the
// candidate's location is tested after the distance band (the distance does not depend on it).
MATCH (startPerson:Person {id: $personId})-[e:KNOWS* SHORTEST 1..10 (_r, _n | WHERE VIS(_r, _n))]-(cand:Person)
WHERE cand.id <> $personId AND VIS(startPerson, cand)
WITH cand, length(e) AS d
WHERE $minPathDistance <= d AND d <= $maxPathDistance
MATCH (cand)-[il:IS_LOCATED_IN]->(ci:City)-[ip:IS_PART_OF]->(co:Country {name: $country})
WHERE VIS(il, ci, ip, co)
MATCH (cand)<-[hc:HAS_CREATOR]-(message:Post:Comment)-[h1:HAS_TAG]->(t1:Tag)-[ht:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE VIS(hc, message, h1, t1, ht, tc)
MATCH (message)-[h2:HAS_TAG]->(tag:Tag)
WHERE VIS(h2, tag)
RETURN cand.id AS `cand.id`, tag.name AS `tag.name`, count(DISTINCT message) AS messageCount
ORDER BY messageCount DESC, `tag.name` ASC, `cand.id` ASC
LIMIT 100
