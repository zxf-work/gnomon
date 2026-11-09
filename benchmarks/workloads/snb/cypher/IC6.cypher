// IC6. Deviation: + VIS on every variable, the KNOWS*1..2 walk as a named path (VISALL).
MATCH (knownTag:Tag { name: $tagName })
WHERE VIS(knownTag)
WITH knownTag.id as knownTagId
MATCH w = (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend AND VISALL(nodes(w)) AND VISALL(relationships(w))
WITH
    knownTagId,
    collect(distinct friend) as friends
UNWIND friends as f
    MATCH (f)<-[h:HAS_CREATOR]-(post:Post),
          (post)-[h1:HAS_TAG]->(t:Tag{id: knownTagId}),
          (post)-[h2:HAS_TAG]->(tag:Tag)
    WHERE NOT t = tag AND VIS(h, post, h1, t, h2, tag)
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
