// IC10 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC10.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH w = (person:Person {id: $personId})-[:KNOWS* TRAIL 2..2 (_r, _n | WHERE VIS(_r, _n))]-(friend),
       (friend)-[fl:IS_LOCATED_IN]->(city:City)
WHERE NOT friend=person AND VIS(person, friend) AND VIS(fl, city)
OPTIONAL MATCH (friend)-[k1:KNOWS]-(person)
WHERE VIS(k1)
WITH person, city, friend, count(k1) AS direct
WHERE direct = 0 AND ((date_part('month', friend.birthday) = $month AND date_part('day', friend.birthday) >= 21) OR
        (date_part('month', friend.birthday) = ($month % 12) + 1 AND date_part('day', friend.birthday) < 22))
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
