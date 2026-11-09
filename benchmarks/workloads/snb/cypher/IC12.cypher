// IC12. Deviation: + VIS on every variable; the tag-class closure as a named path (VISALL).
MATCH w = (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
WHERE (tag.name = $tagClassName OR baseTagClass.name = $tagClassName) AND VISALL(nodes(w)) AND VISALL(relationships(w))
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
    toInteger(personId) ASC
LIMIT 20
