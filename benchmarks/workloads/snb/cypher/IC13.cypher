// IC13. Deviation: + VIS on the end persons and on every element of the shortest path.
MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
WHERE VIS(person1, person2)
OPTIONAL MATCH path = shortestPath((person1)-[:KNOWS*]-(person2))
WHERE VISALL(nodes(path)) AND VISALL(relationships(path))
RETURN
    CASE path IS NULL
        WHEN true THEN -1
        ELSE length(path)
    END AS shortestPathLength
