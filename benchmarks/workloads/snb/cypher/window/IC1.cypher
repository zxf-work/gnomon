// IC1, window setting (rule W1): distance = the smallest length of a KNOWS*1..3 walk from the start person that is
// co-valid with the (friend, city) binding; the university and company lists are correlated parts of that binding
// (G11 verdict 3): an entry is listed when it shares an instant with the friend, the city and some co-valid walk.
MATCH (friend:Person {firstName: $firstName})-[fl:IS_LOCATED_IN]->(friendCity:City)
WHERE friend.id <> $personId AND W_OK(friend, fl, friendCity)
WITH friend, fl, friendCity, W_LOV(friend, fl, friendCity) AS lo, W_HIV(friend, fl, friendCity) AS hi
MATCH w = (p:Person {id: $personId})-[:KNOWS*1..3]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH friend, friendCity, min(length(w)) AS distance,
     collect(DISTINCT {lo: W_LOV(friend, fl, friendCity | nodes(w) + relationships(w)), hi: W_HIV(friend, fl, friendCity | nodes(w) + relationships(w))}) AS walks
ORDER BY distance ASC, friend.lastName ASC, toInteger(friend.id) ASC
LIMIT 20
OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[ul:IS_LOCATED_IN]->(uniCity:City)
WHERE any(wk IN walks WHERE W_INV(wk.lo, wk.hi, studyAt, uni, ul, uniCity))
WITH friend, walks, collect(CASE WHEN uni IS NULL THEN null ELSE [uni.name, toString(studyAt.classYear), uniCity.name] END) AS unis, friendCity, distance
OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[cl:IS_LOCATED_IN]->(companyCountry:Country)
WHERE any(wk IN walks WHERE W_INV(wk.lo, wk.hi, workAt, company, cl, companyCountry))
WITH friend, collect(CASE WHEN company IS NULL then null ELSE [company.name, toString(workAt.workFrom), companyCountry.name] END) AS companies, unis, friendCity, distance
RETURN
    friend.id AS friendId,
    friend.lastName AS friendLastName,
    distance AS distanceFromPerson,
    friend.birthday AS friendBirthday,
    friend.creationDate AS friendCreationDate,
    friend.gender AS friendGender,
    friend.browserUsed AS friendBrowserUsed,
    friend.locationIP AS friendLocationIp,
    split(friend.email, ';') AS friendEmails,
    split(friend.speaks, ';') AS friendLanguages,
    friendCity.name AS friendCityName,
    CASE WHEN size(unis) = 0 THEN null ELSE unis END AS friendUniversities,
    CASE WHEN size(companies) = 0 THEN null ELSE companies END AS friendCompanies
ORDER BY distanceFromPerson ASC, friendLastName ASC, toInteger(friendId) ASC
LIMIT 20
