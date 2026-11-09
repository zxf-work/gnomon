// IC13 for Memgraph: shortestPath() is Memgraph's BFS expansion with a visibility filter lambda. Deviation: + VIS.
MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
WHERE VIS(person1, person2)
OPTIONAL MATCH path = (person1)-[:KNOWS *BFS (e, n | VIS(e, n))]-(person2)
RETURN
    CASE path IS NULL
        WHEN true THEN -1
        ELSE size(relationships(path))
    END AS shortestPathLength
