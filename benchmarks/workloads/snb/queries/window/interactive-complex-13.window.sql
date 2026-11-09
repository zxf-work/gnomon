/* IC13, window setting (rule W1, W1-paths). Deviation: the hop distance of the shortest co-valid walk (persons and
   friendships sharing an instant of the window), by the shared bounded search W_SEARCH (8 rounds). -1 when
   there is none within the bound; 0 for the same person. */
WITH
edges(src, dst, w, lo, hi) AS (
    SELECT k.Person1Id, k.Person2Id, 1, W_LO(k, q), W_HI(k, q)
    FROM Person_knows_Person k JOIN Person q ON q.id = k.Person2Id WHERE W_OK(k, q)
),
W_SEARCH(search, (SELECT p.id, p.id, 0, W_LO(p), W_HI(p) FROM Person p WHERE p.id = :person1Id AND W_OK(p)), edges, 8)
SELECT coalesce((SELECT min(cost) FROM search WHERE node = :person2Id), -1)
;
