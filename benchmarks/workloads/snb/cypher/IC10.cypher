// IC10. Deviation: + VIS on every variable; "not a friend" and "post has a tag the person is interested in" as
// OPTIONAL MATCH counts with VIS; the birthday is a Date (LDBC's text reads epoch milliseconds).
MATCH w = (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
       (friend)-[fl:IS_LOCATED_IN]->(city:City)
WHERE NOT friend=person AND VISALL(nodes(w)) AND VISALL(relationships(w)) AND VIS(fl, city)
OPTIONAL MATCH (friend)-[k1:KNOWS]-(person)
WHERE VIS(k1)
WITH person, city, friend, count(k1) AS direct
WHERE direct = 0 AND ((friend.birthday.month = $month AND friend.birthday.day >= 21) OR
        (friend.birthday.month = ($month % 12) + 1 AND friend.birthday.day < 22))
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[h:HAS_CREATOR]-(post:Post)
WHERE VIS(h, post)
OPTIONAL MATCH (post)-[ht:HAS_TAG]->(t:Tag)<-[hi:HAS_INTEREST]-(person)
WHERE VIS(ht, t, hi)
WITH friend, city, post, count(t) AS common
WITH friend, city, count(post) AS postCount, sum(CASE WHEN common > 0 THEN 1 ELSE 0 END) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10
