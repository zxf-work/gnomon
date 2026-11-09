// BI1 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/BI1.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2.
MATCH (message:Post:Comment)
WHERE message.creationDate < $datetime AND W_OK(message)
WITH count(message) AS totalMessageCountInt
WITH CAST(totalMessageCountInt AS DOUBLE) AS totalMessageCount
MATCH (message:Post:Comment)
WHERE message.creationDate < $datetime AND message.content IS NOT NULL AND W_OK(message)
WITH totalMessageCount, message, date_part('year', message.creationDate) AS year
WITH totalMessageCount, year, label(message) = 'Comment' AS isComment,
  CASE WHEN message.length < 40 THEN 0 WHEN message.length < 80 THEN 1 WHEN message.length < 160 THEN 2 ELSE 3 END AS lengthCategory,
  count(message) AS messageCount,
  sum(message.length) / CAST(count(message) AS DOUBLE) AS averageMessageLength,
  sum(message.length) AS sumMessageLength
RETURN year, isComment, lengthCategory, messageCount, averageMessageLength, sumMessageLength,
  messageCount / totalMessageCount AS percentageOfMessages
ORDER BY year DESC, isComment ASC, lengthCategory ASC
