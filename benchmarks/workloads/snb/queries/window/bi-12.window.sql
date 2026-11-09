/* BI12, window setting (rule W1). Deviation: the per-person count is a stage over (person) bindings with the
   messages as an optional part co-valid with the person (W_IN); the distribution is a second stage. */
WITH person_w_posts AS (
    SELECT Person.id, count(Message.MessageId) as messageCount
      FROM Person
      LEFT JOIN Message
        ON Person.id = Message.CreatorPersonId
       AND Message.content IS NOT NULL
       AND Message.length < :lengthThreshold
       AND Message.creationDate > :startDate
       AND Message.RootPostLanguage IN :languages
       AND W_IN(W_LO(Person), W_HI(Person), Message)
     GROUP BY Person.id, Person.vt_s
)
, message_count_distribution AS (
    SELECT pp.messageCount, count(*) as personCount
      FROM person_w_posts pp
     GROUP BY pp.messageCount
)
SELECT *
  FROM message_count_distribution
ORDER BY personCount DESC, messageCount DESC
;
