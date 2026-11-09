// IC11 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC11.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH (friend:Person)-[workAt:WORK_AT]->(company:Company)-[cl:IS_LOCATED_IN]->(ctry:Country {name: $countryName })
WHERE workAt.workFrom < $workFromYear AND friend.id <> $personId AND W_OK(friend, workAt, company, cl, ctry)
WITH friend, workAt, company, W_LOV(friend, workAt, company, cl, ctry) AS lo, W_HIV(friend, workAt, company, cl, ctry) AS hi
MATCH w = (person:Person {id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE WPRUNE(_r, _n))]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + rels(w))
WITH DISTINCT friend, workAt, company
RETURN
        friend.id AS personId,
        friend.firstName AS personFirstName,
        friend.lastName AS personLastName,
        company.name AS organizationName,
        workAt.workFrom AS organizationWorkFromYear
ORDER BY
        organizationWorkFromYear ASC,
        CAST(personId AS INT64) ASC,
        organizationName DESC
LIMIT 10
