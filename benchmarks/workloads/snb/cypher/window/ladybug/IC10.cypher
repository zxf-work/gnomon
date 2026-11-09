// HAND IC10 for LadybugDB (window, rule W1), from cypher/window/IC10.cypher by the rules R1-R6 and: "some tag-of-interest
// interval e of the post has an instant no direct friendship covers" (any(e IN iext WHERE W_UNCOVERED(...))) is an
// UNWIND over iext (one sentinel row keeps posts without one), since a LadybugDB lambda cannot see an enclosing
// quantifier's variable; the per-post flags are max() over those rows, as Neo4j's max() over its rows; Neo4j's
// collect-then-UNWIND of the walks is the walk rows themselves (LadybugDB refuses the collect, LB-7); the flags are
// materialised by UNWIND [expr] before max() (LB-7 again otherwise); the tag-of-interest OPTIONAL MATCH (two hops,
// its test reads the relationships) runs without WHERE and its test is a flag in the collect, its last node a fresh
// variable tested by id (LB-3 in its OPTIONAL form; the filtered form scored 1-2 low on 3 of 6 SF1 instances), the
// post's interval passed through UNWIND [[plo, phi]] (LadybugDB's binder otherwise calls the flag's collect a nested
// aggregation, LB-7) (D-W3-13).
MATCH (friend:Person)-[fl:IS_LOCATED_IN]->(city:City)
WHERE friend.id <> $personId AND W_OK(friend, fl, city)
  AND ((date_part('month', friend.birthday) = $month AND date_part('day', friend.birthday) >= 21) OR (date_part('month', friend.birthday) = ($month % 12) + 1 AND date_part('day', friend.birthday) < 22))
WITH friend, fl, city, W_LOV(friend, fl, city) AS lo, W_HIV(friend, fl, city) AS hi
MATCH w = (person:Person {id: $personId})-[:KNOWS* TRAIL 2..2 (_r, _n | WHERE WPRUNE(_r, _n))]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + rels(w))
WITH DISTINCT friend, city, W_LOV(friend, fl, city | nodes(w) + rels(w)) AS wlo, W_HIV(friend, fl, city | nodes(w) + rels(w)) AS whi
OPTIONAL MATCH (friend)-[kd:KNOWS]-(q0:Person {id: $personId})
WHERE W_INV(wlo, whi, kd, q0)
WITH friend, city, wlo, whi, collect(CASE WHEN kd IS NULL THEN null ELSE W_EXT(kd, q0) END) AS kext
WHERE W_UNCOVERED(wlo, whi, kext)
WITH friend, city, {lo: wlo, hi: whi, ext: kext} AS wk
OPTIONAL MATCH (friend)<-[h:HAS_CREATOR]-(post:Post)
WHERE W_INV(wk.lo, wk.hi, h, post)
WITH friend, city, wk, post,
     CASE WHEN post IS NULL THEN null WHEN W_LOV(h, post) > wk.lo THEN W_LOV(h, post) ELSE wk.lo END AS plo,
     CASE WHEN post IS NULL THEN null WHEN W_HIV(h, post) < wk.hi THEN W_HIV(h, post) ELSE wk.hi END AS phi
UNWIND [[plo, phi]] AS pp
OPTIONAL MATCH (post)-[ht:HAS_TAG]->(t:Tag)<-[hi2:HAS_INTEREST]-(pz:Person)
WITH friend, city, wk, post, plo, phi, ht, t, hi2, (pz.id = $personId AND W_INV(pp[1], pp[2], ht, t, hi2)) AS ok
WITH friend, city, wk, post, plo, phi, collect(CASE WHEN ok THEN W_EXT(ht, t, hi2) ELSE null END) AS iext
UNWIND list_concat(coalesce(iext, CAST([] AS INT64[][])), [[CAST(-1 AS INT64), CAST(-1 AS INT64)]]) AS e
WITH friend, city, wk, post, plo, phi, iext, e,
     CASE WHEN e[1] > plo THEN e[1] ELSE plo END AS elo, CASE WHEN e[2] < phi THEN e[2] ELSE phi END AS ehi
UNWIND [CASE WHEN post IS NOT NULL AND e[1] >= 0 AND W_UNCOVERED(elo, ehi, wk.ext) THEN 1 ELSE 0 END] AS com
UNWIND [CASE WHEN post IS NOT NULL AND W_UNCOVERED(plo, phi, list_concat(coalesce(wk.ext, CAST([] AS INT64[][])), coalesce(iext, CAST([] AS INT64[][])))) THEN 1 ELSE 0 END] AS unc
WITH friend, city, post, max(com) AS isCommon, max(unc) AS isUncommon
WITH friend, city, sum(isCommon) AS commonPostCount, sum(isUncommon) AS uncommonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - uncommonPostCount AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10
