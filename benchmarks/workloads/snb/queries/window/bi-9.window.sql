/* BI9, window setting (rule W1). Deviation: the per-thread message count is a stage over valid (message) bindings
   (rule 5); the (person, post) binding is co-valid (W_OK). */
WITH
MPP AS (SELECT RootPostId, count(*) as MessageCount FROM Message WHERE Message.creationDate BETWEEN :startDate AND :endDate GROUP BY RootPostId)
SELECT Person.id AS "person.id"
     , Person.firstName AS "person.firstName"
     , Person.lastName AS "person.lastName"
     , count(Post.id) AS threadCount
     , sum(MPP.MessageCount) AS messageCount
  FROM Person
  JOIN Post_View Post
    ON Person.id = Post.CreatorPersonId
  JOIN MPP
    ON Post.id = MPP.RootPostId
 WHERE Post.creationDate BETWEEN :startDate AND :endDate
   AND W_OK(Person, Post)
 GROUP BY Person.id, Person.firstName, Person.lastName
 ORDER BY messageCount DESC, Person.id
 LIMIT 100
;
