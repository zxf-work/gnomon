/* IS4, sequenced. Deviation: none needed. */
SELECT coalesce(imageFile, content, ''), creationDate
FROM Message
WHERE id = :messageId
;
