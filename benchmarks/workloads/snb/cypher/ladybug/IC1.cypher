// HAND IC1 for LadybugDB (instant settings), from cypher/IC1.cypher. Changes (D-W3-13): shortestPath((p)-[:KNOWS*1..3]-(f))
// with VISALL on its elements is LadybugDB's `* SHORTEST 1..3` with the VIS test in the step predicate (filtered while
// searched, as Neo4j applies a shortestPath predicate), in the same MATCH as its filtered endpoints; two rounds: the
// 20 friends (distance, last name, id) are the first statement's rows (LB-9: an aggregation right after WITH ... ORDER
// BY ... LIMIT ignores the order), their node keys the second's list; the friend's columns are carried as values and
// the node re-matched by its key for each later pattern (a node carried through grouping WITHs lost its properties,
// LB-6); each two-hop OPTIONAL MATCH is two one-hop OPTIONAL MATCHes, an entry listed only when the whole chain matched
// (LB-3); an empty collect() is NULL (LB-8), which the size() test maps to NULL as Neo4j's empty list; toString ->
// CAST(... AS STRING), split -> string_split.
MATCH (p:Person {id: $personId})-[e:KNOWS* SHORTEST 1..3 (_r, _n | WHERE VIS(_r, _n))]-(friend:Person {firstName: $firstName})
WHERE NOT p.id = friend.id AND VIS(p, friend)
WITH friend.gid AS fg, friend.id AS fid, friend.lastName AS fln, min(length(e)) AS distance
RETURN fg, distance, fln, fid
ORDER BY distance ASC, fln ASC, fid ASC
LIMIT 20
// --- ARG fgs
MATCH (p:Person {id: $personId})-[e:KNOWS* SHORTEST 1..3 (_r, _n | WHERE VIS(_r, _n))]-(friend:Person {firstName: $firstName})
WHERE friend.gid IN $fgs AND NOT p.id = friend.id AND VIS(p, friend)
WITH friend.gid AS fg, min(length(e)) AS distance
MATCH (friend:Person)-[fl:IS_LOCATED_IN]->(friendCity:City)
WHERE friend.gid = fg AND VIS(fl, friendCity)
WITH fg, distance, friend.id AS fid, friend.lastName AS fln, friend.birthday AS fbd, friend.creationDate AS fcd,
     friend.gender AS fge, friend.browserUsed AS fbu, friend.locationIP AS fip, friend.email AS fem,
     friend.speaks AS fsp, friendCity.name AS fcity
MATCH (friend:Person)
WHERE friend.gid = fg
OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)
WHERE VIS(studyAt, uni)
OPTIONAL MATCH (uni)-[ul:IS_LOCATED_IN]->(uniCity:City)
WHERE VIS(ul, uniCity)
WITH fg, distance, fid, fln, fbd, fcd, fge, fbu, fip, fem, fsp, fcity,
     collect(CASE WHEN uniCity IS NULL THEN NULL ELSE [uni.name, CAST(studyAt.classYear AS STRING), uniCity.name] END) AS unis
MATCH (friend:Person)
WHERE friend.gid = fg
OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)
WHERE VIS(workAt, company)
OPTIONAL MATCH (company)-[cl:IS_LOCATED_IN]->(companyCountry:Country)
WHERE VIS(cl, companyCountry)
WITH distance, fid, fln, fbd, fcd, fge, fbu, fip, fem, fsp, fcity, unis,
     collect(CASE WHEN companyCountry IS NULL THEN NULL ELSE [company.name, CAST(workAt.workFrom AS STRING), companyCountry.name] END) AS companies
RETURN
    fid AS friendId,
    fln AS friendLastName,
    distance AS distanceFromPerson,
    fbd AS friendBirthday,
    fcd AS friendCreationDate,
    fge AS friendGender,
    fbu AS friendBrowserUsed,
    fip AS friendLocationIp,
    string_split(fem, ';') AS friendEmails,
    string_split(fsp, ';') AS friendLanguages,
    fcity AS friendCityName,
    CASE WHEN size(unis) = 0 THEN NULL ELSE unis END AS friendUniversities,
    CASE WHEN size(companies) = 0 THEN NULL ELSE companies END AS friendCompanies
ORDER BY distanceFromPerson ASC, friendLastName ASC, friendId ASC
LIMIT 20
