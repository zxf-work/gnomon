// IC1 for Memgraph: shortestPath() is Memgraph's BFS expansion with a visibility filter lambda. Deviation: + VIS on every variable and on the shortest path's elements;
// email/speaks are stored ';'-joined (the SQL reference's form) and split on return.
MATCH (p:Person {id: $personId}), (friend:Person {firstName: $firstName})
       WHERE NOT p.id = friend.id AND VIS(p, friend)
       WITH p, friend
       MATCH path = (p)-[:KNOWS *BFS 1..3 (e, n | VIS(e, n))]-(friend)

       WITH min(length(path)) AS distance, friend
ORDER BY
    distance ASC,
    friend.lastName ASC,
    toInteger(friend.id) ASC
LIMIT 20
MATCH (friend)-[fl:IS_LOCATED_IN]->(friendCity:City)
WHERE VIS(fl, friendCity)
OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[ul:IS_LOCATED_IN]->(uniCity:City)
WHERE VIS(studyAt, uni, ul, uniCity)
WITH friend, collect(
    CASE
        WHEN uni IS NULL THEN null
        ELSE [uni.name, toString(studyAt.classYear), uniCity.name]
    END ) AS unis, friendCity, distance
OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[cl:IS_LOCATED_IN]->(companyCountry:Country)
WHERE VIS(workAt, company, cl, companyCountry)
WITH friend, collect(
    CASE
        WHEN company IS NULL then null
        ELSE [company.name, toString(workAt.workFrom), companyCountry.name]
    END ) AS companies, unis, friendCity, distance
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
ORDER BY
    distanceFromPerson ASC,
    friendLastName ASC,
    toInteger(friendId) ASC
LIMIT 20
