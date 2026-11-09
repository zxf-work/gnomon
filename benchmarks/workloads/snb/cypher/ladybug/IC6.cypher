// IC6 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC6.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5, R6 (h1.rid <> h2.rid).
MATCH (knownTag:Tag { name: $tagName })
WHERE VIS(knownTag)
WITH knownTag.id as knownTagId
MATCH w = (person:Person { id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE VIS(_r, _n))]-(friend)
WHERE NOT person=friend AND VIS(person, friend)
WITH
    knownTagId,
    collect(distinct friend) as friends
UNWIND friends as f
    MATCH (f)<-[h:HAS_CREATOR]-(post:Post),
          (post)-[h1:HAS_TAG]->(t:Tag{id: knownTagId}),
          (post)-[h2:HAS_TAG]->(tag:Tag)
    WHERE h1.rid <> h2.rid AND NOT t = tag AND VIS(h, post, h1, t, h2, tag)
    WITH
        tag.name as tagName,
        count(post) as postCount
RETURN
    tagName,
    postCount
ORDER BY
    postCount DESC,
    tagName ASC
LIMIT 10
