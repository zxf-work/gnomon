/* BI15, window setting (rule W1, W1-paths). Deviation: the interaction weights are a stage over valid (friendship,
   message, reply) bindings in forums of the date range (rule 5), joined to each friendship on its key; the
   cheapest path is searched over co-valid walks (persons and friendships) carrying (lo, hi), by the shared bounded search W_SEARCH (8 rounds). -1 when no co-valid path exists. */
WITH
myForums(id) AS (
    SELECT id FROM Forum f WHERE f.creationDate BETWEEN :startDate AND :endDate AND W_OK(f)
),
mm AS (
    SELECT least(msg.CreatorPersonId, reply.CreatorPersonId) AS src, greatest(msg.CreatorPersonId, reply.CreatorPersonId) AS dst,
           sum(CASE WHEN msg.ParentMessageId IS NULL THEN 10 ELSE 5 END) AS w
    FROM Person_knows_Person pp, Message msg, Message reply
    WHERE pp.person1id = msg.CreatorPersonId
      AND pp.person2id = reply.CreatorPersonId
      AND reply.ParentMessageId = msg.MessageId
      AND EXISTS (SELECT * FROM myForums f WHERE f.id = msg.containerforumid)
      AND EXISTS (SELECT * FROM myForums f WHERE f.id = reply.containerforumid)
      AND W_OK(pp, msg, reply)
    GROUP BY src, dst
),
edges(src, dst, w, lo, hi) AS (
    SELECT pp.person1id, pp.person2id, 10::double precision / (coalesce(mm.w, 0) + 10), W_LO(pp, q), W_HI(pp, q)
    FROM Person_knows_Person pp JOIN Person q ON q.id = pp.person2id
    LEFT JOIN mm ON least(pp.person1id, pp.person2id) = mm.src AND greatest(pp.person1id, pp.person2id) = mm.dst
    WHERE W_OK(pp, q)
),
W_SEARCH(search, (SELECT p.id, p.id, 0::double precision, W_LO(p), W_HI(p) FROM Person p WHERE p.id = :person1Id AND W_OK(p)), edges, 8)
SELECT coalesce(min(cost), -1) FROM search WHERE node = :person2Id
;
