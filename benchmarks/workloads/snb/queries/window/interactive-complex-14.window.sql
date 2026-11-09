/* IC14 (v2), window setting (rule W1, W1-paths). Deviation: the interaction weights are a stage over valid
   (friendship, reply, parent) bindings (rule 5), joined to each friendship on its key; the cheapest co-valid
   walk by W_SEARCH (8 rounds). The answer is the minimum path weight (the path is one of possibly several, so
   it is not compared). No path gives no row. */
WITH
pathb(a, b, w) AS (
    SELECT least(c.creatorpersonid, p.creatorpersonid), greatest(c.creatorpersonid, p.creatorpersonid),
           greatest(round(40 - sqrt(count(*)))::bigint, 1)
    FROM message c, message p, person_knows_person k
    WHERE c.parentmessageid = p.id AND k.person1id = c.creatorpersonid AND k.person2id = p.creatorpersonid
      AND W_OK(c, p, k)
    GROUP BY 1, 2
),
edges(src, dst, w, lo, hi) AS (
    SELECT k.Person1Id, k.Person2Id, pb.w, W_LO(k, q), W_HI(k, q)
    FROM Person_knows_Person k JOIN Person q ON q.id = k.Person2Id
    JOIN pathb pb ON pb.a = least(k.Person1Id, k.Person2Id) AND pb.b = greatest(k.Person1Id, k.Person2Id)
    WHERE W_OK(k, q)
),
W_SEARCH(search, (SELECT p.id, p.id, 0::bigint, W_LO(p), W_HI(p) FROM Person p WHERE p.id = :person1Id AND W_OK(p)), edges, 8)
SELECT min(cost) AS pathWeight FROM search WHERE node = :person2Id
HAVING count(*) > 0   -- no path: no row, as at the instant settings and in every system (an aggregate alone gives one NULL row)
;
