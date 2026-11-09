// IC11. Deviation: + VIS on every variable, the KNOWS*1..2 walk as a named path (VISALL).
MATCH w = (person:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
WHERE not(person=friend) AND VISALL(nodes(w)) AND VISALL(relationships(w))
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
        toInteger(personId) ASC,
        organizationName DESC
LIMIT 10
