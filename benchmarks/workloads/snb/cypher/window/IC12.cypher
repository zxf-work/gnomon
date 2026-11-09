// IC12, window setting (rule W1): one binding (start person, friendship, friend, creator edge, comment, reply edge,
// post, tag edge, tag) co-valid with the tag-class walk.
MATCH (p0:Person {id: $personId })-[k:KNOWS]-(friend:Person)<-[h:HAS_CREATOR]-(comment:Comment)-[r:REPLY_OF]->(post:Post)-[ht:HAS_TAG]->(tag:Tag),
      w = (tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
WHERE (tag.name = $tagClassName OR baseTagClass.name = $tagClassName)
  AND W_OK(p0, k, friend, h, comment, r, post, ht, tag | nodes(w) + relationships(w))
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY replyCount DESC, toInteger(personId) ASC
LIMIT 20
