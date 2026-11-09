// IC11 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC11.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH w = (person:Person {id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE VIS(_r, _n))]-(friend:Person)
WHERE not(person=friend) AND VIS(person, friend)
WITH DISTINCT friend
MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[cl:IS_LOCATED_IN]->(ctry:Country {name: $countryName })
WHERE workAt.workFrom < $workFromYear AND VIS(workAt, company, cl, ctry)
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
