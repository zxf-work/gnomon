// IC1 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC1.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH (friend:Person {firstName: $firstName})-[fl:IS_LOCATED_IN]->(friendCity:City)
WHERE friend.id <> $personId AND W_OK(friend, fl, friendCity)
WITH friend, fl, friendCity, W_LOV(friend, fl, friendCity) AS lo, W_HIV(friend, fl, friendCity) AS hi
MATCH w = (p:Person {id: $personId})-[:KNOWS* TRAIL 1..3 (_r, _n | WHERE WPRUNE(_r, _n))]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + rels(w))
WITH friend, friendCity, min(length(w)) AS distance,
     collect(DISTINCT {lo: W_LOV(friend, fl, friendCity | nodes(w) + rels(w)), hi: W_HIV(friend, fl, friendCity | nodes(w) + rels(w))}) AS walks
ORDER BY distance ASC, friend.lastName ASC, CAST(friend.id AS INT64) ASC
LIMIT 20
OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[ul:IS_LOCATED_IN]->(uniCity:City)
WHERE any(wk IN walks WHERE W_INV(wk.lo, wk.hi, studyAt, uni, ul, uniCity))
WITH friend, walks, collect(CASE WHEN uni IS NULL THEN null ELSE [uni.name, CAST(studyAt.classYear AS STRING), uniCity.name] END) AS unis, friendCity, distance
OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[cl:IS_LOCATED_IN]->(companyCountry:Country)
WHERE any(wk IN walks WHERE W_INV(wk.lo, wk.hi, workAt, company, cl, companyCountry))
WITH friend, collect(CASE WHEN company IS NULL then null ELSE [company.name, CAST(workAt.workFrom AS STRING), companyCountry.name] END) AS companies, unis, friendCity, distance
RETURN
    friend.id AS friendId,
    friend.lastName AS friendLastName,
    distance AS distanceFromPerson,
    friend.birthday AS friendBirthday,
    friend.creationDate AS friendCreationDate,
    friend.gender AS friendGender,
    friend.browserUsed AS friendBrowserUsed,
    friend.locationIP AS friendLocationIp,
    string_split(friend.email, ';') AS friendEmails,
    string_split(friend.speaks, ';') AS friendLanguages,
    friendCity.name AS friendCityName,
    CASE WHEN size(unis) = 0 THEN null ELSE unis END AS friendUniversities,
    CASE WHEN size(companies) = 0 THEN null ELSE companies END AS friendCompanies
ORDER BY distanceFromPerson ASC, friendLastName ASC, CAST(friendId AS INT64) ASC
LIMIT 20
