// IC11, window setting (rule W1): the friend walk (1..2 hops) must be co-valid with the (friend, job, company,
// country) binding.
MATCH (friend:Person)-[workAt:WORK_AT]->(company:Company)-[cl:IS_LOCATED_IN]->(ctry:Country {name: $countryName })
WHERE workAt.workFrom < $workFromYear AND friend.id <> $personId AND W_OK(friend, workAt, company, cl, ctry)
WITH friend, workAt, company, W_LOV(friend, workAt, company, cl, ctry) AS lo, W_HIV(friend, workAt, company, cl, ctry) AS hi
MATCH w = (person:Person {id: $personId })-[:KNOWS*1..2]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH DISTINCT friend, workAt, company
RETURN
        friend.id AS personId,
        friend.firstName AS personFirstName,
        friend.lastName AS personLastName,
        company.name AS organizationName,
        workAt.workFrom AS organizationWorkFromYear
ORDER BY
        organizationWorkFromYear ASC,
        toInteger(personId) ASC,
        organizationName DESC
LIMIT 10
