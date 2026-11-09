// IC10, window setting (rule W1): a binding is (friend, city, a co-valid 2-hop walk carried with its own interval)
// at an instant without a direct friendship (W1's NOT). The two post counts are correlated parts of that binding
// (G11 verdict 3): a post is common when, at such an instant of some walk, it shares it with a tag of interest, and
// uncommon when, at such an instant, it carries none.
MATCH (friend:Person)-[fl:IS_LOCATED_IN]->(city:City)
WHERE friend.id <> $personId AND W_OK(friend, fl, city)
  AND ((friend.birthday.month = $month AND friend.birthday.day >= 21) OR (friend.birthday.month = ($month % 12) + 1 AND friend.birthday.day < 22))
WITH friend, fl, city, W_LOV(friend, fl, city) AS lo, W_HIV(friend, fl, city) AS hi
MATCH w = (person:Person {id: $personId})-[:KNOWS*2..2]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH DISTINCT friend, city, W_LOV(friend, fl, city | nodes(w) + relationships(w)) AS wlo, W_HIV(friend, fl, city | nodes(w) + relationships(w)) AS whi
OPTIONAL MATCH (friend)-[kd:KNOWS]-(q0:Person {id: $personId})
WHERE W_INV(wlo, whi, kd, q0)
WITH friend, city, wlo, whi, collect(CASE WHEN kd IS NULL THEN null ELSE W_EXT(kd, q0) END) AS kext
WHERE W_UNCOVERED(wlo, whi, kext)
WITH friend, city, collect({lo: wlo, hi: whi, ext: kext}) AS walks
UNWIND walks AS wk
OPTIONAL MATCH (friend)<-[h:HAS_CREATOR]-(post:Post)
WHERE W_INV(wk.lo, wk.hi, h, post)
WITH friend, city, wk, post,
     CASE WHEN post IS NULL THEN null WHEN W_LOV(h, post) > wk.lo THEN W_LOV(h, post) ELSE wk.lo END AS plo,
     CASE WHEN post IS NULL THEN null WHEN W_HIV(h, post) < wk.hi THEN W_HIV(h, post) ELSE wk.hi END AS phi
OPTIONAL MATCH (post)-[ht:HAS_TAG]->(t:Tag)<-[hi2:HAS_INTEREST]-(:Person {id: $personId})
WHERE W_INV(plo, phi, ht, t, hi2)
WITH friend, city, wk, post, plo, phi, collect(CASE WHEN ht IS NULL THEN null ELSE W_EXT(ht, t, hi2) END) AS iext
WITH friend, city, post,
     max(CASE WHEN post IS NOT NULL AND any(e IN iext WHERE
             W_UNCOVERED(CASE WHEN e[0] > plo THEN e[0] ELSE plo END, CASE WHEN e[1] < phi THEN e[1] ELSE phi END, wk.ext))
         THEN 1 ELSE 0 END) AS isCommon,
     max(CASE WHEN post IS NOT NULL AND W_UNCOVERED(plo, phi, wk.ext + iext) THEN 1 ELSE 0 END) AS isUncommon
WITH friend, city, sum(isCommon) AS commonPostCount, sum(isUncommon) AS uncommonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - uncommonPostCount AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10
