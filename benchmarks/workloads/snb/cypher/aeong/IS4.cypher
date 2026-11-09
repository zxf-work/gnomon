// T-LDBC IS4 for AeonG, native transaction-time form: AeonG's own clock carries belief time (TT AS),
// valid time is a property predicate. Deviation from LDBC: + VISTT on every variable, + TT AS per MATCH; the SQL reference's column order and coalesce order (D-W3-5).
MATCH (m:Message {id: $messageId })
WHERE VISTT(m) TT AS $AT
RETURN
    coalesce(m.imageFile, m.content, '') as messageContent,
    m.creationDate as messageCreationDate
