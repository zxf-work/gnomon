// IC12 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC12.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH w = (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]->(baseTagClass:TagClass)
WHERE (tag.name = $tagClassName OR baseTagClass.name = $tagClassName) AND VIS(tag, baseTagClass)
WITH collect(tag.id) as tags
MATCH (p0:Person {id: $personId })-[k:KNOWS]-(friend:Person)<-[h:HAS_CREATOR]-(comment:Comment)-[r:REPLY_OF]->(post:Post)-[ht:HAS_TAG]->(tag:Tag)
WHERE tag.id in tags AND VIS(p0, k, friend, h, comment, r, post, ht, tag)
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY
    replyCount DESC,
    CAST(personId AS INT64) ASC
LIMIT 20
